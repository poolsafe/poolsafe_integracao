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


def format_utc_iso(seconds=None):
    dt = datetime.fromtimestamp(seconds, timezone.utc) if seconds is not None else datetime.now(timezone.utc)
    return dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def prepare_evidence(frame, max_dim=640, quality=70, max_bytes=250_000):
    """Gera JPEG compacto sem anotações. Falhas secundárias não impedem o evento."""
    try:
        import cv2
        h, w = frame.shape[:2]
        if h <= 0 or w <= 0:
            log.warning('Evidência descartada: dimensões de frame inválidas (%sx%s)', w, h)
            return None
        scale = min(1.0, max_dim / max(h, w))
        target_w = max(1, int(w * scale))
        target_h = max(1, int(h * scale))
        small = cv2.resize(frame, (target_w, target_h))
        ok, jpeg = cv2.imencode('.jpg', small, [cv2.IMWRITE_JPEG_QUALITY, quality])
        if not ok or jpeg is None:
            log.warning('Evidência descartada: falha na codificação JPEG')
            return None
        raw = jpeg.tobytes() if hasattr(jpeg, 'tobytes') else bytes(jpeg)
        if len(raw) > max_bytes:
            log.warning('Evidência descartada: limite de tamanho excedido (%d > %d bytes)', len(raw), max_bytes)
            return None
        return {
            'mimeType': 'image/jpeg',
            'base64': base64.b64encode(raw).decode('ascii')
        }
    except Exception as exc:
        log.warning('Evidência descartada por exceção (%s)', type(exc).__name__)
        return None


def setup_logs(directory):
    directory.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    service_handler = RotatingFileHandler(directory / 'servico.log', maxBytes=5_000_000, backupCount=5)
    service_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logging.getLogger().addHandler(service_handler)
    logging.getLogger('pika').setLevel(logging.WARNING)

    audit = logging.getLogger('deteccoes')
    audit.setLevel(logging.INFO)
    audit.propagate = False
    audit_handler = RotatingFileHandler(directory / 'deteccoes.jsonl', maxBytes=10_000_000, backupCount=5)
    audit.addHandler(audit_handler)
    return audit, [service_handler, audit_handler]


def close_logs(handlers):
    for h in handlers:
        try:
            h.close()
            logging.getLogger().removeHandler(h)
            logging.getLogger('deteccoes').removeHandler(h)
        except Exception:
            pass


def new_motor(c):
    return Motor(c.frames, c.normal_frames, c.cooldown, c.max_gap, c.lost_reset)


def main():
    c = Config()
    c.validate()
    directory = Path(c.data_dir)
    audit, log_handlers = setup_logs(directory)
    stopping = threading.Event()
    reader = None
    cap = None
    publisher = None
    frame_number = 0
    outbox = Outbox(directory / 'outbox.sqlite3', c.max_pending)

    try:
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

        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: stopping.set())
        publisher = Publisher(c, outbox, stopping=stopping) if c.publish else None
        if publisher:
            publisher.start()

        motor = new_motor(c)
        run_id = str(uuid.uuid4())
        generation = 0
        hb_last = float('-inf')
        hb_lock = threading.Lock()
        started = time.monotonic()
        wait_log = started

        def on_stream_frame(observed_iso, observed_epoch):
            nonlocal hb_last
            with hb_lock:
                if observed_epoch - hb_last < c.heartbeat_interval:
                    return
                hb_last = observed_epoch

            camera_code = c.codigo_integracao or c.camera_id
            expires_epoch = observed_epoch + c.heartbeat_ttl
            hb_body = {
                'schemaVersion': '1',
                'eventKey': str(uuid.uuid4()),
                'type': 'CAMERA_HEARTBEAT',
                'cameraCode': camera_code,
                'observedAt': observed_iso,
                'expiresAt': format_utc_iso(expires_epoch),
                'technicalData': {
                    'test': c.test
                }
            }
            if publisher:
                publisher.heartbeat(hb_body, expires_epoch)
            else:
                log.info('Heartbeat local cameraCode=%s observedAt=%s', camera_code, observed_iso)

        if c.source_mode == 'stream':
            reader = StreamReader(c.source, stopping, on_frame_decoded=on_stream_frame)
            reader.start()
        else:
            cap = cv2.VideoCapture(c.source)
            if not cap.isOpened():
                raise ValueError('Não foi possível abrir o vídeo')
        fps = cap.get(cv2.CAP_PROP_FPS) if cap else None
        if cap and (not fps or fps <= 0):
            cap.release()
            raise ValueError('FPS inválido: recodifique o vídeo com taxa conhecida')

        while not stopping.is_set():
            if publisher and publisher.fatal_error:
                log.error('Encerramento controlado acionado: publicador finalizou com erro irrecuperável (%s). Preservando outbox.',
                          publisher.fatal_error)
                stopping.set()
                break
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
            else:
                ok, frame = cap.read()
                if not ok:
                    log.info('Fim do vídeo, ou erro de leitura; conferir quantidade de frames no ensaio')
                    break
                video_time = frame_number / fps
                if stopping.wait(max(0, started + video_time - time.monotonic())):
                    break
                logical_time = video_time
                observed = format_utc_iso()  # Instante de reprodução com Z
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
                    if tid not in selected or score > selected[tid]['confianca']:
                        selected[tid] = d
                        observations[tid] = label == c.risk_class
            decisions = motor.update(observations, frame_number, logical_time)
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

                event_key = str(uuid.uuid4())
                camera_code = c.codigo_integracao or c.camera_id
                if c.alert_ttl > 0:
                    observed_epoch = datetime.fromisoformat(observed.replace('Z', '+00:00')).timestamp()
                    expires = observed_epoch + c.alert_ttl
                else:
                    expires = 0.0

                event = {
                    'schemaVersion': '1',
                    'eventKey': event_key,
                    'cameraCode': camera_code,
                    'type': 'POSSIVEL_RISCO',
                    'severity': 'CRITICA',
                    'detectedAt': observed,
                    'confidence': round(float(selected[tid]['confianca']), 4),
                    'modelVersion': c.model_version,
                    'technicalData': {
                        'test': c.test,
                        'source': 'video' if cap else 'stream',
                        'runId': run_id,
                        'segment': generation,
                        'trackId': tid,
                        'processedFrame': frame_number,
                        'videoTimeSeconds': round(video_time, 4) if video_time is not None else None,
                        'modelSha256': sha
                    }
                }
                if c.evidence:
                    ev = prepare_evidence(frame)
                    if ev:
                        event['evidence'] = ev

                if publisher:
                    outbox.put(event, expires)
                log.info('Evento local=%s cameraCode=%s track=%s video_time=%s publicação=%s',
                         event_key, camera_code, tid, video_time, c.publish)
    finally:
        stopping.set()
        if cap:
            cap.release()
        if reader:
            reader.join(timeout=6)
        if publisher:
            if not getattr(publisher, 'fatal_error', None):
                deadline = time.monotonic() + c.drain_seconds
                while publisher.is_alive() and outbox.count() and time.monotonic() < deadline:
                    time.sleep(.1)
            if hasattr(publisher, 'stop_event'):
                publisher.stop_event.set()
            if hasattr(publisher, 'join'):
                publisher.join(timeout=12)
        log.info('Encerrado: frames_processados=%s pendentes_outbox=%s', frame_number, outbox.count())
        close_logs(log_handlers)
        if publisher and getattr(publisher, 'fatal_error', None):
            raise RuntimeError(f'Publicador encerrou com erro irrecuperável: {publisher.fatal_error}')


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        log.error('Serviço interrompido (%s). Conferir arquivos, configuração, permissões e recursos.',
                  type(exc).__name__)
        raise SystemExit(1)
