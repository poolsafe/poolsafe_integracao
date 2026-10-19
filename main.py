"""PoolSafe: produtor de visão computacional para UMA câmera.

Fluxo: best.pt -> YOLO11/ByteTrack -> regra temporal -> outbox -> RabbitMQ.
O Python NÃO conhece senha do app, pausa, sessão, destinatários, MySQL ou FCM.
A política de notificação/pausa pertence ao n8n e à API existente.
"""
import base64
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import signal
import threading
import time
import uuid
from datetime import datetime, timezone

from captura import StreamReader, utc_now
from config import Config
from regras import Motor
from transporte import Outbox, Publisher

log = logging.getLogger('poolsafe')


def iso_at(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec='milliseconds')


def setup_logs(directory):
    directory.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    # Logs com rotação: não ocupar disco ilimitadamente durante monitoramento contínuo.
    handler = RotatingFileHandler(directory / 'servico.log', maxBytes=5_000_000, backupCount=5)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logging.getLogger().addHandler(handler)
    logging.getLogger('pika').setLevel(logging.WARNING)
    audit = logging.getLogger('deteccoes')
    audit.setLevel(logging.INFO)
    audit.propagate = False
    audit.addHandler(RotatingFileHandler(directory / 'deteccoes.jsonl',
                                         maxBytes=10_000_000, backupCount=5))
    return audit


def new_motor(c):
    return Motor(c.frames, c.normal_frames, c.cooldown, c.max_gap, c.lost_reset)


def main():
    c = Config()
    c.validate()
    directory = Path(c.data_dir)
    audit = setup_logs(directory)
    # Bibliotecas pesadas são carregadas só na execução; os testes da lógica não
    # precisam baixar pesos, instalar CUDA ou abrir uma câmera.
    import cv2
    from ultralytics import YOLO

    model = YOLO(c.model_path)
    names = {int(k): str(v).strip().lower() for k, v in model.names.items()}
    expected = {c.risk_class, c.normal_class}
    if not expected.issubset(names.values()):
        raise ValueError(f'Classes incompatíveis: best contém {names}; esperado {expected}')
    if any(v not in expected for v in names.values()):
        raise ValueError('Esta implementação exige o modelo com apenas as duas classes configuradas')
    with open(c.model_path, 'rb') as weights:
        sha = hashlib.file_digest(weights, 'sha256').hexdigest()
    log.info('Modelo versão=%s sha256=%s classes=%s device=%s', c.model_version, sha, names, c.device)
    log.info('Modo=%s teste=%s publicação=%s; frames=%s cooldown=%ss',
             c.source_mode, c.test, c.publish, c.frames, c.cooldown)
    if c.source_mode == 'video' and not c.test:
        raise ValueError('Vídeo gravado exige TEST_MODE=true para não se passar por evento real')

    stopping = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping.set())
    outbox = Outbox(directory / 'outbox.sqlite3', c.max_pending)
    publisher = Publisher(c, outbox) if c.publish else None
    if publisher:
        publisher.start()
    reader = None
    cap = None
    motor = new_motor(c)
    run_id = str(uuid.uuid4())
    generation = 0
    frame_number = 0
    hb_last = float('-inf')
    started = time.monotonic()
    wait_log = started
    if c.source_mode == 'stream':
        reader = StreamReader(c.source, stopping)
        reader.start()
    else:
        cap = cv2.VideoCapture(c.source)
        if not cap.isOpened():
            raise ValueError('Não foi possível abrir o vídeo')
    fps = cap.get(cv2.CAP_PROP_FPS) if cap else None
    if cap and (not fps or fps <= 0):
        cap.release()
        raise ValueError('FPS inválido: recodifique o vídeo com taxa conhecida')

    try:
        while not stopping.is_set():
            video_time = None
            if reader:
                packet = reader.take()
                if packet is None:
                    if time.monotonic() - wait_log >= 30:
                        log.warning('Aguardando novo frame: nenhum heartbeat inventado')
                        wait_log = time.monotonic()
                    stopping.wait(.01)
                    continue
                capture_sequence, segment, frame, captured_mono, observed = packet
                if time.monotonic() - captured_mono > c.max_gap:
                    # Não inferir sobre um frame que já envelheceu no slot do Python.
                    continue
                if segment != generation:
                    generation = segment
                    # Novo predictor implica tracker novo. IDs nunca são identidades
                    # globais: o evento também inclui run_id e segmento.
                    model.predictor = None
                    motor = new_motor(c)
                    log.info('Novo segmento de captura: %s; tracking reiniciado', segment)
                logical_time = captured_mono
                if captured_mono - hb_last >= c.heartbeat_interval:
                    hb_last = captured_mono
                    observed_epoch = datetime.fromisoformat(observed).timestamp()
                    body = {'schema_version': 1, 'event_id': str(uuid.uuid4()),
                            'tipo': 'heartbeat', 'camera_id': c.camera_id,
                            'codigo_integracao': c.codigo_integracao or c.camera_id,
                            'observado_em': observed,
                            'expira_em': iso_at(observed_epoch + c.heartbeat_ttl), 'teste': c.test}
                    if publisher:
                        publisher.heartbeat(body, observed_epoch + c.heartbeat_ttl)
                    else:
                        log.info('Heartbeat local camera_id=%s observado_em=%s', c.camera_id, observed)
            else:
                ok, frame = cap.read()
                if not ok:
                    log.info('Fim do vídeo, ou erro de leitura; conferir quantidade de frames no ensaio')
                    break
                # Reproduzir no ritmo nominal evita enviar em poucos segundos
                # os eventos de um vídeo de vários minutos durante a demonstração.
                video_time = frame_number / fps
                if stopping.wait(max(0, started + video_time - time.monotonic())):
                    break
                logical_time = video_time
                observed = utc_now()  # Instante de reprodução, NÃO data original da gravação.
                capture_sequence = frame_number + 1

            frame_number += 1
            process_start = time.monotonic()
            result = model.track(frame, persist=True, tracker='bytetrack.yaml',
                                 conf=c.conf, iou=c.iou, imgsz=c.imgsz,
                                 device=c.device, verbose=False)[0]
            observations, selected, detections = {}, {}, []
            boxes = result.boxes
            if boxes is not None and len(boxes):
                xyxy = boxes.xyxy.cpu().tolist()
                classes = boxes.cls.cpu().tolist()
                scores = boxes.conf.cpu().tolist()
                ids = boxes.id.cpu().tolist() if boxes.id is not None else [None] * len(boxes)
                for bounds, cls, score, tid in zip(xyxy, classes, scores, ids):
                    label = names[int(cls)]
                    tid = int(tid) if tid is not None else None
                    d = {'track_id': tid, 'classe': label, 'confianca': float(score),
                         'bbox_xyxy': [round(v, 2) for v in bounds]}
                    detections.append(d)
                    if tid is None:
                        continue  # Detecção sem ID é auditada, mas não confirma por identidade.
                    # Se houver caixas concorrentes com mesmo ID, usar a mais confiante.
                    if tid not in selected or score > selected[tid]['confianca']:
                        selected[tid] = d
                        observations[tid] = label == c.risk_class
            decisions = motor.update(observations, frame_number, logical_time)
            # Há uma linha para TODO frame processado, incluindo vazio e sem ID.
            # Frames descartados pelo slot não entram nesta contagem.
            audit.info(json.dumps({'run_id': run_id, 'segmento': generation,
                                   'frame_processado': frame_number,
                                   'frame_capturado': capture_sequence,
                                   'observado_em': observed, 'video_time_s': video_time,
                                   'processamento_s': time.monotonic() - process_start,
                                   'deteccoes': detections, 'decisoes': decisions}))
            for tid, decision in decisions:
                if decision != 'confirmado':
                    log.info('Confirmação suprimida por cooldown camera=%s track=%s', c.camera_id, tid)
                    continue
                event_id = str(uuid.uuid4())
                confirmed_epoch = time.time()
                expires = datetime.fromisoformat(observed).timestamp() + c.alert_ttl
                event = {'schema_version': 1, 'event_id': event_id, 'tipo': 'alerta',
                         'camera_id': c.camera_id,
                         'codigo_integracao': c.codigo_integracao or c.camera_id,
                         'observado_em': observed, 'confirmado_em': iso_at(confirmed_epoch),
                         'expira_em': iso_at(expires), 'teste': c.test,
                         'origem': 'video' if cap else 'stream',
                         'run_id': run_id, 'segmento': generation,
                         'frame_processado': frame_number, 'video_time_s': video_time,
                         'modelo': {'versao': c.model_version, 'sha256': sha},
                         'deteccao': selected[tid],
                         'regra': {'frames_consecutivos': c.frames, 'conf_minima': c.conf,
                                   'frames_normais_encerrar': c.normal_frames,
                                   'cooldown_s': c.cooldown, 'max_intervalo_s': c.max_gap},
                         'evidencia': None}
                if c.evidence:
                    # Opcional e privado: JPEG reduzido dentro da mensagem, sem URL pública.
                    # O contrato n8n precisa aceitar/decodificar o campo antes de habilitar.
                    h, w = frame.shape[:2]
                    scale = min(1., 640 / max(h, w))
                    small = cv2.resize(frame, (max(1, int(w * scale)), max(1, int(h * scale))))
                    ok, jpeg = cv2.imencode('.jpg', small, [cv2.IMWRITE_JPEG_QUALITY, 70])
                    if ok and len(jpeg) <= 250_000:
                        event['evidencia'] = {'mime_type': 'image/jpeg',
                                             'jpeg_base64': base64.b64encode(jpeg).decode()}
                if publisher:
                    outbox.put(event, expires)
                # Não escrever imagem/base64 nos logs. event_id permite correlação.
                log.info('Evento local=%s camera=%s track=%s video_time=%s publicação=%s',
                         event_id, c.camera_id, tid, video_time, c.publish)
    finally:
        stopping.set()
        if cap:
            cap.release()
        if reader:
            reader.join(timeout=6)
        if publisher:
            deadline = time.monotonic() + c.drain_seconds
            while publisher.is_alive() and outbox.count() and time.monotonic() < deadline:
                time.sleep(.1)
            publisher.stop_event.set()
            publisher.join(timeout=12)
        log.info('Encerrado: frames_processados=%s pendentes_outbox=%s', frame_number, outbox.count())


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Mensagens de bibliotecas podem conter SOURCE com senha. Registrar só o tipo.
        log.error('Serviço interrompido (%s). Conferir arquivos, configuração, permissões e recursos.',
                  type(exc).__name__)
        raise SystemExit(1)
