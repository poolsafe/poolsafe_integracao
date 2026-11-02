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
from engine_adapter import MotorV4, validate_model_classes, ModelCompatibilityError, V4Error, detections_from_result
from transporte import Outbox, Publisher
from observabilidade import JsonFormatter, Observer, log_event, log_risk_event, reference
from capturas_inspecao import CaptureWriter
from runtime_support import configure_runtime, synchronize_device

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
    formatter = JsonFormatter()
    logging.basicConfig(level=logging.INFO)
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter)

    # Log cumulativo no diretório geral de dados
    service_handler = RotatingFileHandler(directory / 'servico.log', maxBytes=5_000_000, backupCount=5, encoding='utf-8')
    service_handler.setFormatter(formatter)
    logging.getLogger().addHandler(service_handler)

    # Log específico desta execução isolada
    run_handler = RotatingFileHandler(run_dir / 'servico.log', maxBytes=5_000_000, backupCount=2, encoding='utf-8')
    run_handler.setFormatter(formatter)
    logging.getLogger().addHandler(run_handler)

    logging.getLogger('pika').setLevel(logging.WARNING)
    handlers = [service_handler, run_handler]

    audit = None
    if log_mode == 'development':
        audit = logging.getLogger('deteccoes')
        audit.setLevel(logging.INFO)
        audit.propagate = False
        audit_handler = RotatingFileHandler(run_dir / 'deteccoes.jsonl', maxBytes=10_000_000, backupCount=5, encoding='utf-8')
        audit_handler.setFormatter(formatter)
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


def new_motor(c, fps=None):
    return MotorV4(c, fps=fps)


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
    from collections import deque
    local_events_summary = deque(maxlen=1000)
    observer = Observer(c, run_id)
    captures = CaptureWriter(c, run_dir, run_id)

    total_proc_time = 0.0
    min_frame_proc_time = None
    max_frame_proc_time = None
    first_frame_proc_time = None
    subsequent_proc_time_sum = 0.0
    subsequent_proc_count = 0

    try:
        # Bibliotecas pesadas carregadas só em runtime
        import cv2
        import torch
        from ultralytics import YOLO
        configure_runtime(c, cv2, torch)

        with open(c.model_path, 'rb') as weights:
            sha = hashlib.file_digest(weights, 'sha256').hexdigest()
        if c.model_sha256 and sha != c.model_sha256.lower():
            raise ValueError('MODEL_SHA256 não corresponde aos pesos montados')
        model = YOLO(c.model_path)
        names = validate_model_classes(model.names)

        log.info('Execução iniciada run_id=%s log_mode=%s', run_id, c.log_mode)
        log.info('Modelo versão=%s sha256=%s classes=%s device=%s', c.model_version, sha, names, c.device)
        log.info('Modo=%s teste=%s publicação=%s politica_motor=%s',
                 c.source_mode, c.test, c.publish, c.engine_policy())

        if c.source_mode == 'video' and not c.test:
            raise ValueError('Vídeo gravado exige TEST_MODE=true para não se passar por evento real')

        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: stopping.set())

        motor = new_motor(c)
        publisher = Publisher(c, outbox, stopping=stopping) if c.publish else None
        if publisher:
            publisher.start()

        generation = 0
        hb_last = float('-inf')
        hb_lock = threading.Lock()
        started = time.monotonic()
        wait_log = started

        def on_stream_frame(observed_iso, observed_epoch):
            nonlocal hb_last
            observer.captured()
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

        if c.source_mode == 'video':
            motor = new_motor(c, fps=video_fps)
        observer.flush(outbox, force=True)

        while not stopping.is_set():
            observer.flush(outbox)
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
                observer.captured()

            frame_number += 1
            process_start = time.monotonic()
            frame_age_s = max(0.0, process_start-captured_mono) if reader else 0.0
            synchronize_device(c, torch)
            inference_started = time.perf_counter()

            result = model.track(frame, persist=True, tracker='bytetrack.yaml',
                                 conf=c.inference_conf, iou=c.iou, imgsz=c.imgsz,
                                 device=c.device, verbose=False)[0]

            synchronize_device(c, torch)
            inference_s = time.perf_counter()-inference_started
            try:
                detections = detections_from_result(result, names)
            except V4Error as exc:
                log_event(log, 'v4.failure', level=logging.ERROR, run_ref=reference(run_id),
                          frame=frame_number, segment=generation, error_code=exc.code,
                          error_type=type(exc).__name__)
                raise
            observations = {d['track_id']: d['classe'] for d in detections if d['track_id'] is not None}

            trace_validation = c.log_mode != 'homologation' and (c.log_mode == 'development' or frame_number % c.v4_log_every == 0)
            if (trace_validation or c.log_mode == 'homologation') and c.source_mode == 'stream' and motor.last_time is not None and logical_time-motor.last_time > c.max_gap:
                log_event(log, 'v4.sampling_gap', level=logging.WARNING,
                          gap_s=round(logical_time-motor.last_time, 3), frame=frame_number)
            if trace_validation:
                log_event(log, 'v4.request', run_ref=reference(run_id), frame=frame_number,
                          detections=len(detections), segment=generation)
            try:
                decisions = motor.update_detections(detections, frame_number, logical_time)
            except V4Error as exc:
                log_event(log, 'v4.failure', level=logging.ERROR, run_ref=reference(run_id),
                          frame=frame_number, segment=generation, error_code=exc.code,
                          error_type=type(exc).__name__, engine_ms=round(motor.duration_s*1000, 3))
                raise
            if trace_validation or (decisions and c.log_mode != 'homologation'):
                log_event(log, 'v4.response', run_ref=reference(run_id), frame=frame_number,
                          engine_ms=round(motor.duration_s*1000, 3), rows=len(motor.rows),
                          emitted=sum(d.status == 'confirmado' for d in decisions),
                          transitions=len(motor.transitions), handoffs=len(motor.continuity))
            if motor.continuity:
                log_event(log, 'v4.handoff', frame=frame_number, transfers=len(motor.continuity))
                for transition in motor.continuity:
                    if transition.get('transicao') == 'interrupcao_lacuna_stream':
                        log_event(log, 'v4.continuity_interrupted', level=logging.WARNING,
                                  frame=frame_number, segment=generation,
                                  gap_s=transition['intervalo_s'], limit_s=transition['limite_s'])
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
                    'motor_v4': {'linhas': motor.rows, 'transicoes': motor.transitions, 'continuidade': motor.continuity} if c.engine_version == 'v4' else None,
                    'engine_version': 'v4'
                }
                audit.info('v4.frame', extra={'event_name': 'v4.frame', 'fields': audit_record})

            # Processamento de decisões de risco
            capture_events = []
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

                engine_event = getattr(d, 'event', None)
                confirmed_class = engine_event['classe_origem']
                confirmed_conf = round(float(engine_event['confidence']), 4)

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

                if c.log_mode != 'homologation':
                    log.info('Evento local=%s severity=%s classe=%s cameraCode=%s track=%s video_time=%s publicação=%s',
                             event_key, severity, confirmed_class, camera_code, tid, video_time, c.publish)

                log_risk_event(log, c.log_mode, classe=confirmed_class, track_id=tid, observed_at=observed,
                          event_ref=reference(event_key), run_ref=reference(run_id),
                          frame=frame_number, severity=severity, engine='v4',
                          event_kind=engine_event['tipo_evento'],
                          evidence_count=len(engine_event.get('frames_evidencias', [])))
                summary_event = {
                    'event_ref': reference(event_key),
                    'severity': severity,
                    'classe': confirmed_class,
                    'track_id': tid,
                    'frame_processado': frame_number,
                    'video_time_s': round(video_time, 4) if video_time is not None else None,
                    'detectedAt': observed,
                    'confidence': confirmed_conf
                }
                with (run_dir / 'eventos.jsonl').open('a', encoding='utf-8') as event_log:
                    event_log.write(json.dumps(summary_event, ensure_ascii=False) + '\n')
                local_events_summary.append(summary_event)
                capture_events.append(summary_event)

            captures.submit(frame, detections, motor.rows, events=capture_events,
                            segment=generation, processed_frame=frame_number,
                            captured_frame=capture_sequence, observed_at=observed,
                            video_time_s=video_time, sample_time=logical_time)
            observer.completed(capture_sequence, inference_s, motor.duration_s,
                               time.monotonic()-process_start, frame_age_s,
                               detections=detections, frame=frame_number, segment=generation)

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
        captures.close()
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

        try:
            observer.flush(outbox, force=True)
            observer.stopped(exit_reason)
        except Exception as exc_health:
            log.warning('Falha ao persistir saúde: %s', type(exc_health).__name__)

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
            'source': '[redacted]',
            'configuracao_efetiva': {
                'camera_id': c.camera_id,
                'codigo_integracao': c.codigo_integracao,
                'source_mode': c.source_mode,
                'model_version': c.model_version,
                'device': c.device,
                'conf': c.inference_conf,
                'politica_motor': c.engine_policy(),
                'iou': c.iou,
                'imgsz': c.imgsz,
                'frames_para_alerta': None,
                'frames_normais_encerrar': c.normal_frames,
                'cooldown_s': c.cooldown,
                'max_intervalo_s': c.max_gap,
                'test_mode': c.test,
                'allow_test_publish': c.allow_test_publish,
                'publish_rabbit': c.publish,
                'include_evidence': c.evidence,
                'max_pending_events': c.max_pending,
                'homologation_log_every_n_frames': c.homologation_log_every,
                'captures_enabled': c.captures_enabled,
                'captures_interval_s': c.captures_interval,
                'captures_max_count': c.captures_max_count,
                'captures_max_bytes': c.captures_max_bytes,
            },
            'capturas': {'quantidade': captures.count, 'bytes': captures.bytes,
                         'suspensas_por_limite': captures.suspended,
                         'avisos': sorted(captures.warned), 'indice': 'capturas/indice.jsonl'},
            'modelo': {
                'versao': c.model_version,
                'sha256': sha,
                'classes': names,
            },
            'revisao_codigo': __import__('os').getenv('BUILD_REVISION') or get_git_revision(),
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
                'lista_eventos': list(local_events_summary),
                'lista_eventos_limite': 1000,
                'arquivo_eventos_completos': 'eventos.jsonl',
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
    logging.basicConfig(level=logging.INFO)
    for handler in logging.getLogger().handlers:
        handler.setFormatter(JsonFormatter())
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

