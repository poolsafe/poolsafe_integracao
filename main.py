"""PoolSafe: produtor de visão computacional para UMA câmera.

Fluxo: best.pt -> YOLO11/ByteTrack -> regra temporal multi-classe -> outbox -> RabbitMQ.
O Python NÃO conhece senha do app, pausa, sessão, destinatários, MySQL ou FCM.
A política de notificação/pausa pertence ao n8n e à API existente.
"""
import base64
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import re
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from captura import StreamReader, utc_now
from config import Config
from regras import Motor, validate_model_classes, ModelCompatibilityError, Decisao
from transporte import Outbox, Publisher

log = logging.getLogger('poolsafe')


def format_utc_iso(seconds: Optional[float] = None) -> str:
    dt = datetime.fromtimestamp(seconds, timezone.utc) if seconds is not None else datetime.now(timezone.utc)
    return dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def redact_url(url: str) -> str:
    """Mascara senhas em URLs RTSP/HTTP para registros seguros."""
    if not url:
        return ''
    return re.sub(r'(://[^:]+:)[^@]+(@)', r'\1***\2', str(url))


def get_git_revision() -> Optional[str]:
    """Obtém o hash SHA do commit Git atual, se disponível, sem inventar valores."""
    try:
        import subprocess
        res = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            capture_output=True, text=True, timeout=2,
            cwd=str(Path(__file__).resolve().parent)
        )
        if res.returncode == 0:
            commit = res.stdout.strip()
            if commit:
                return commit
    except Exception:
        pass
    return None


def compute_iou(b1: List[float], b2: List[float]) -> float:
    """Calcula a interseção sobre união (IoU) entre duas bounding boxes [x1, y1, x2, y2]."""
    ix1 = max(b1[0], b2[0])
    iy1 = max(b1[1], b2[1])
    ix2 = min(b1[2], b2[2])
    iy2 = min(b1[3], b2[3])
    iw = max(0.0, ix2 - ix1)
    ih = max(0.0, iy2 - iy1)
    intersection = iw * ih
    if intersection <= 0:
        return 0.0
    area1 = (b1[2] - b1[0]) * (b1[3] - b1[1])
    area2 = (b2[2] - b2[0]) * (b2[3] - b2[1])
    union = area1 + area2 - intersection
    return intersection / union if union > 0 else 0.0


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


def setup_logs(directory: Path, run_dir: Path, log_mode: str):
    """Configura o sistema de log isolando execuções por run_id.

    - Production: registra apenas eventos operacionais, início/fim, modelo e sumário.
    - Development: inclui auditoria detalhada por frame em deteccoes.jsonl.
    """
    directory.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    # Log cumulativo no diretório geral de dados
    service_handler = RotatingFileHandler(directory / 'servico.log', maxBytes=5_000_000, backupCount=5, encoding='utf-8')
    service_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logging.getLogger().addHandler(service_handler)

    # Log específico desta execução isolada
    run_handler = RotatingFileHandler(run_dir / 'servico.log', maxBytes=5_000_000, backupCount=2, encoding='utf-8')
    run_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logging.getLogger().addHandler(run_handler)

    logging.getLogger('pika').setLevel(logging.WARNING)
    handlers = [service_handler, run_handler]

    audit = None
    if log_mode == 'development':
        audit = logging.getLogger('deteccoes')
        audit.setLevel(logging.INFO)
        audit.propagate = False
        audit_handler = RotatingFileHandler(run_dir / 'deteccoes.jsonl', maxBytes=10_000_000, backupCount=5, encoding='utf-8')
        audit.addHandler(audit_handler)
        handlers.append(audit_handler)

    return audit, handlers


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
    run_id = str(uuid.uuid4())
    run_dir = directory / 'runs' / run_id
    audit, log_handlers = setup_logs(directory, run_dir, c.log_mode)

    stopping = threading.Event()
    reader = None
    cap = None
    publisher = None
    frame_number = 0
    # Outbox SQLite preservada no diretório raiz persistente
    outbox = Outbox(directory / 'outbox.sqlite3', c.max_pending)

    exit_reason = 'desconhecido'
    exit_detail = ''
    started_epoch = time.time()
    started_mono = time.monotonic()
    sha = 'indisponivel'
    names = {}
    video_fps = None
    video_duration = None
    video_total_frames = None

    eventos_por_severidade = {'ATENCAO': 0, 'CRITICA': 0}
    supressoes_por_severidade = {'ATENCAO': 0, 'CRITICA': 0}
    local_events_summary = []

    total_proc_time = 0.0
    min_frame_proc_time = None
    max_frame_proc_time = None
    first_frame_proc_time = None
    subsequent_proc_time_sum = 0.0
    subsequent_proc_count = 0

    try:
        # Bibliotecas pesadas carregadas só em runtime
        import cv2
        from ultralytics import YOLO

        model = YOLO(c.model_path)
        raw_names = {int(k): str(v).strip().lower() for k, v in model.names.items()}
        names = validate_model_classes(raw_names)

        with open(c.model_path, 'rb') as weights:
            sha = hashlib.file_digest(weights, 'sha256').hexdigest()

        log.info('Execução iniciada run_id=%s log_mode=%s', run_id, c.log_mode)
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
                exit_reason = 'falha_abertura_video'
                raise ValueError('Não foi possível abrir o vídeo')
            video_fps = cap.get(cv2.CAP_PROP_FPS)
            prop_frame_count = getattr(cv2, 'CAP_PROP_FRAME_COUNT', None)
            total_raw = cap.get(prop_frame_count) if prop_frame_count is not None else None
            if total_raw and total_raw > 0:
                video_total_frames = int(total_raw)
                if video_fps and video_fps > 0:
                    video_duration = round(video_total_frames / video_fps, 4)

            if not video_fps or video_fps <= 0:
                cap.release()
                exit_reason = 'fps_invalido_video'
                raise ValueError('FPS inválido: recodifique o vídeo com taxa conhecida')

        while not stopping.is_set():
            if publisher and publisher.fatal_error:
                exit_reason = 'erro_fatal_publicador'
                exit_detail = str(publisher.fatal_error)
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
                    # Não inferir sobre frame envelhecido no slot
                    continue
                if segment != generation:
                    generation = segment
                    model.predictor = None
                    motor = new_motor(c)
                    log.info('Novo segmento de captura: %s; tracking reiniciado', segment)
                logical_time = captured_mono
            else:
                ok, frame = cap.read()
                if not ok:
                    exit_reason = 'fim_leitura_video'
                    if video_total_frames is not None and video_total_frames > 0:
                        if frame_number >= video_total_frames:
                            exit_detail = f"Leitura de todos os {frame_number} frames esperados concluída com sucesso."
                        else:
                            exit_detail = f"Leitura retornou False antes do total esperado ({frame_number}/{video_total_frames} frames)."
                    else:
                        exit_detail = f"Leitura encerrou no frame {frame_number} (total de frames do vídeo desconhecido)."
                    log.info('Fim da leitura do vídeo: %s', exit_detail)
                    break

                video_time = frame_number / video_fps
                if stopping.wait(max(0, started + video_time - time.monotonic())):
                    exit_reason = 'interrupcao_solicitada'
                    break
                logical_time = video_time
                observed = format_utc_iso()  # Instante de reprodução UTC com Z
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
                    d = {
                        'track_id': tid,
                        'classe': label,
                        'confianca': float(score),
                        'bbox_xyxy': [round(v, 2) for v in bounds]
                    }
                    detections.append(d)
                    if tid is None:
                        continue  # Auditado, mas não confirma por tracking
                    if tid not in selected or score > selected[tid]['confianca']:
                        selected[tid] = d
                        # Registra o nome da classe observada
                        observations[tid] = label

            # Verificação diagnóstica de sobreposição alta entre classes distintas (apenas em development)
            overlaps = []
            if c.log_mode == 'development' and len(detections) > 1:
                for i in range(len(detections)):
                    for j in range(i + 1, len(detections)):
                        d1, d2 = detections[i], detections[j]
                        if d1['classe'] != d2['classe']:
                            iou_val = compute_iou(d1['bbox_xyxy'], d2['bbox_xyxy'])
                            if iou_val >= 0.70:
                                overlaps.append({
                                    'track_id_1': d1['track_id'],
                                    'classe_1': d1['classe'],
                                    'track_id_2': d2['track_id'],
                                    'classe_2': d2['classe'],
                                    'iou': round(iou_val, 3),
                                    'bbox_1': d1['bbox_xyxy'],
                                    'bbox_2': d2['bbox_xyxy']
                                })

            decisions = motor.update(observations, frame_number, logical_time)
            frame_proc_time = time.monotonic() - process_start

            # Agregação de latências sem manter medições infinitas em memória
            total_proc_time += frame_proc_time
            if min_frame_proc_time is None or frame_proc_time < min_frame_proc_time:
                min_frame_proc_time = frame_proc_time
            if max_frame_proc_time is None or frame_proc_time > max_frame_proc_time:
                max_frame_proc_time = frame_proc_time

            if frame_number == 1:
                first_frame_proc_time = frame_proc_time
            else:
                subsequent_proc_time_sum += frame_proc_time
                subsequent_proc_count += 1

            # Auditoria JSONL por frame apenas em modo development
            if audit:
                audit_record = {
                    'run_id': run_id,
                    'segmento': generation,
                    'frame_processado': frame_number,
                    'frame_capturado': capture_sequence,
                    'observado_em': observed,
                    'video_time_s': round(video_time, 4) if video_time is not None else None,
                    'processamento_s': round(frame_proc_time, 6),
                    'deteccoes': detections,
                    'sem_deteccao': len(detections) == 0,
                    'deteccoes_sem_id': sum(1 for d in detections if d['track_id'] is None),
                    'contagens_por_track': {
                        tid: motor.get_track_state(tid) for tid in observations
                    },
                    'decisoes': [
                        {'track_id': d.track_id, 'status': d.status, 'severity': d.severity}
                        for d in decisions
                    ],
                    'sobreposicoes_classes_distintas': overlaps
                }
                audit.info(json.dumps(audit_record, ensure_ascii=False))

            # Processamento de decisões de risco
            for d in decisions:
                tid, decision_status, severity = d.track_id, d.status, d.severity
                if decision_status != 'confirmado':
                    supressoes_por_severidade[severity] = supressoes_por_severidade.get(severity, 0) + 1
                    log.info('Confirmação suprimida por cooldown camera=%s track=%s severidade=%s',
                             c.camera_id, tid, severity)
                    continue

                eventos_por_severidade[severity] = eventos_por_severidade.get(severity, 0) + 1
                # Cada emissão de atenção ou crítica gera sua própria eventKey exclusiva
                event_key = str(uuid.uuid4())
                camera_code = c.codigo_integracao or c.camera_id
                if c.alert_ttl > 0:
                    observed_epoch = datetime.fromisoformat(observed.replace('Z', '+00:00')).timestamp()
                    expires = observed_epoch + c.alert_ttl
                else:
                    expires = 0.0

                confirmed_class = selected[tid]['classe']
                confirmed_conf = round(float(selected[tid]['confianca']), 4)

                event = {
                    'schemaVersion': '1',
                    'eventKey': event_key,
                    'cameraCode': camera_code,
                    'type': 'POSSIVEL_RISCO',
                    'severity': severity,
                    'detectedAt': observed,
                    'confidence': confirmed_conf,
                    'modelVersion': c.model_version,
                    'technicalData': {
                        'test': c.test,
                        'source': 'video' if cap else 'stream',
                        'runId': run_id,
                        'segment': generation,
                        'trackId': tid,
                        'processedFrame': frame_number,
                        'videoTimeSeconds': round(video_time, 4) if video_time is not None else None,
                        'modelSha256': sha,
                        'detectedClass': confirmed_class,
                        'severity': severity
                    }
                }
                if c.evidence:
                    ev = prepare_evidence(frame)
                    if ev:
                        event['evidence'] = ev

                if publisher:
                    outbox.put(event, expires)

                log.info('Evento local=%s severity=%s classe=%s cameraCode=%s track=%s video_time=%s publicação=%s',
                         event_key, severity, confirmed_class, camera_code, tid, video_time, c.publish)

                local_events_summary.append({
                    'eventKey': event_key,
                    'severity': severity,
                    'classe': confirmed_class,
                    'track_id': tid,
                    'frame_processado': frame_number,
                    'video_time_s': round(video_time, 4) if video_time is not None else None,
                    'detectedAt': observed,
                    'confidence': confirmed_conf
                })

    except ModelCompatibilityError as exc:
        exit_reason = f'erro_compatibilidade_modelo_{exc.code}'
        exit_detail = str(exc)
        log.error('Erro de compatibilidade do modelo [%s]: %s', exc.code, exc)
        raise
    except Exception as exc:
        if exit_reason == 'desconhecido':
            exit_reason = f'erro_execucao_{type(exc).__name__}'
            exit_detail = str(exc)
        raise
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

        total_duration = time.monotonic() - started_mono
        effective_fps = round(frame_number / total_duration, 2) if total_duration > 0 else None
        avg_frame_time = (total_proc_time / frame_number) if frame_number > 0 else None
        subsequent_avg = (subsequent_proc_time_sum / subsequent_proc_count) if subsequent_proc_count > 0 else None

        if exit_reason == 'desconhecido' and not stopping.is_set():
            exit_reason = 'concluido'
        elif exit_reason == 'desconhecido':
            exit_reason = 'interrompido'

        # Geração do resumo JSON com métricas consolidadas
        resumo = {
            'run_id': run_id,
            'inicio': format_utc_iso(started_epoch),
            'fim': format_utc_iso(time.time()),
            'motivo_encerramento': exit_reason,
            'detalhe_encerramento': exit_detail,
            'log_mode': c.log_mode,
            'source_mode': c.source_mode,
            'source': redact_url(c.source),
            'configuracao_efetiva': {
                'camera_id': c.camera_id,
                'codigo_integracao': c.codigo_integracao,
                'source_mode': c.source_mode,
                'model_version': c.model_version,
                'device': c.device,
                'conf': c.conf,
                'iou': c.iou,
                'imgsz': c.imgsz,
                'frames_para_alerta': c.frames,
                'frames_normais_encerrar': c.normal_frames,
                'cooldown_s': c.cooldown,
                'max_intervalo_s': c.max_gap,
                'lost_reset_s': c.lost_reset,
                'test_mode': c.test,
                'allow_test_publish': c.allow_test_publish,
                'publish_rabbit': c.publish,
                'include_evidence': c.evidence,
                'max_pending_events': c.max_pending,
            },
            'modelo': {
                'versao': c.model_version,
                'sha256': sha,
                'classes': names,
            },
            'revisao_codigo': get_git_revision(),
            'video': {
                'fps': video_fps,
                'duracao_nominal_s': video_duration,
                'total_frames_declarado': video_total_frames,
            } if c.source_mode == 'video' else None,
            'processamento': {
                'frames_processados': frame_number,
                'duracao_total_s': round(total_duration, 4),
                'fps_efetivo': effective_fps,
                'tempo_primeiro_frame_s': round(first_frame_proc_time, 4) if first_frame_proc_time is not None else None,
                'tempo_medio_frames_subsequentes_s': round(subsequent_avg, 4) if subsequent_avg is not None else None,
                'tempo_min_frame_s': round(min_frame_proc_time, 4) if min_frame_proc_time is not None else None,
                'tempo_medio_frame_s': round(avg_frame_time, 4) if avg_frame_time is not None else None,
                'tempo_max_frame_s': round(max_frame_proc_time, 4) if max_frame_proc_time is not None else None,
            },
            'eventos': {
                'confirmados_por_severidade': eventos_por_severidade,
                'suprimidos_por_severidade': supressoes_por_severidade,
                'lista_eventos': local_events_summary,
            },
            'outbox': {
                'pendencias_finais': outbox.count(),
            }
        }

        try:
            (run_dir / 'resumo.json').write_text(json.dumps(resumo, indent=2, ensure_ascii=False), encoding='utf-8')
            (directory / 'latest_run').write_text(run_id, encoding='utf-8')
        except Exception as exc_save:
            log.warning('Não foi possível gravar resumo.json (%s)', type(exc_save).__name__)

        log.info('Encerrado: run_id=%s motivo=%s frames_processados=%s pendentes_outbox=%s',
                 run_id, exit_reason, frame_number, outbox.count())
        close_logs(log_handlers)
        if publisher and getattr(publisher, 'fatal_error', None):
            raise RuntimeError(f'Publicador encerrou com erro irrecuperável: {publisher.fatal_error}')


if __name__ == '__main__':
    try:
        main()
    except ModelCompatibilityError as exc:
        log.error('Serviço interrompido [código=%s]: %s (encontradas=%s, esperadas=%s)',
                  exc.code, exc, exc.found, exc.expected)
        raise SystemExit(1)
    except Exception as exc:
        log.error('Serviço interrompido (%s: %s). Conferir arquivos, configuração, permissões e recursos.',
                  type(exc).__name__, exc)
        raise SystemExit(1)

