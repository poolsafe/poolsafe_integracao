"""Outbox durável + publicação numa thread exclusiva.

A topologia do RabbitMQ (exchange direct, filas, bindings e DLQ) é gerenciada
pela infraestrutura externa. O produtor não realiza declarações ativas de recursos.
A conexão Pika BlockingConnection é usada exclusivamente pela thread Publisher.
A publicação utiliza publisher confirms e mandatory=True. Mensagens de risco são
persistentes (delivery_mode=2). O eventKey é usado como message_id AMQP.
"""
import json
import logging
import sqlite3
import ssl
import threading
import time
from pathlib import Path
from contextlib import contextmanager
from observabilidade import log_event, reference

log = logging.getLogger(__name__)


def adapt_legacy_payload(body):
    """Converte com segurança payloads legados para o novo contrato de API."""
    if body.get('schemaVersion') == '1':
        if not body.get('eventKey') or not body.get('cameraCode'):
            raise ValueError('Envelope novo sem eventKey ou cameraCode')
        return body

    tipo = body.get('tipo') or body.get('type')
    if tipo in ('heartbeat', 'CAMERA_HEARTBEAT'):
        event_key = body.get('eventKey') or body.get('event_id')
        camera_code = body.get('cameraCode') or body.get('codigo_integracao') or body.get('camera_id')
        if not event_key or not camera_code:
            raise ValueError('Heartbeat legado sem identificadores mínimos')
        return {
            'schemaVersion': '1',
            'eventKey': event_key,
            'type': 'CAMERA_HEARTBEAT',
            'cameraCode': camera_code,
            'observedAt': body.get('observedAt') or body.get('observado_em'),
            'expiresAt': body.get('expiresAt') or body.get('expira_em'),
            'technicalData': {
                'test': body.get('teste', True)
            }
        }

    event_key = body.get('eventKey') or body.get('event_id')
    if not event_key:
        raise ValueError('Evento legado sem event_id/eventKey')

    camera_code = body.get('cameraCode') or body.get('codigo_integracao') or body.get('camera_id')
    if not camera_code:
        camera_code = 'camera_legada'

    observed_at = body.get('detectedAt') or body.get('observado_em') or '1970-01-01T00:00:00.000Z'

    det = body.get('deteccao') or {}
    model = body.get('modelo') or {}
    conf = body.get('confidence')
    if conf is None:
        conf = det.get('confianca', 0.0)

    severity = body.get('severity') or body.get('severidade') or 'CRITICA'
    detected_class = det.get('classe') or body.get('detectedClass') or body.get('classe') or 'pessoa_queda'

    adapted = {
        'schemaVersion': '1',
        'eventKey': event_key,
        'cameraCode': camera_code,
        'type': 'POSSIVEL_RISCO',
        'severity': severity,
        'detectedAt': observed_at,
        'confidence': float(conf),
        'modelVersion': str(body.get('modelVersion') or model.get('versao') or 'candidato'),
        'technicalData': {
            'test': body.get('teste', True),
            'source': body.get('origem', 'video'),
            'runId': body.get('run_id', ''),
            'segment': body.get('segmento', 0),
            'trackId': det.get('track_id'),
            'processedFrame': body.get('frame_processado'),
            'videoTimeSeconds': body.get('video_time_s'),
            'modelSha256': model.get('sha256', ''),
            'detectedClass': detected_class
        }
    }
    evidencia = body.get('evidence') or body.get('evidencia')
    if evidencia:
        mime = evidencia.get('mimeType') or evidencia.get('mime_type') or 'image/jpeg'
        b64 = evidencia.get('base64') or evidencia.get('jpeg_base64')
        if b64:
            adapted['evidence'] = {
                'mimeType': mime,
                'base64': b64
            }
    return adapted


class Outbox:
    def __init__(self, path, max_pending=10000):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path, self.max_pending = str(path), max_pending
        self.lock = threading.Lock()
        with self.connection() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE IF NOT EXISTS pendentes '
                       '(id TEXT PRIMARY KEY, expires REAL NOT NULL, body TEXT NOT NULL)')

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=5)
        try:
            db.execute('PRAGMA synchronous=FULL')
            with db:
                yield db
        finally:
            db.close()

    def put(self, body, expires):
        event_id = body.get('eventKey') or body.get('event_id')
        if not event_id:
            raise ValueError('Payload sem identificador (eventKey/event_id)')
        with self.lock, self.connection() as db:
            if db.execute('SELECT COUNT(*) FROM pendentes').fetchone()[0] >= self.max_pending:
                raise RuntimeError('Outbox cheia: verificar broker e disco antes de retomar')
            db.execute('INSERT INTO pendentes VALUES (?, ?, ?)',
                       (event_id, expires, json.dumps(body, ensure_ascii=False)))

    def first(self):
        with self.lock, self.connection() as db:
            return db.execute('SELECT id, expires, body FROM pendentes ORDER BY rowid LIMIT 1').fetchone()

    def remove(self, event_id):
        with self.lock, self.connection() as db:
            db.execute('DELETE FROM pendentes WHERE id=?', (event_id,))

    def count(self):
        with self.lock, self.connection() as db:
            return db.execute('SELECT COUNT(*) FROM pendentes').fetchone()[0]


class Publisher(threading.Thread):
    def __init__(self, cfg, outbox, stopping=None):
        super().__init__(name='rabbit-publisher', daemon=True)
        self.cfg, self.outbox = cfg, outbox
        self.main_stopping = stopping
        self.stop_event = threading.Event()
        self.hb_lock = threading.Lock()
        self.pending_hb = None
        self.conn = self.channel = None
        self.fatal_error = None

    def heartbeat(self, body, expires):
        with self.hb_lock:
            self.pending_hb = (body, expires)

    def connect(self):
        import pika
        c = self.cfg
        tls = None
        if c.rabbit_tls:
            tls = pika.SSLOptions(ssl.create_default_context(cafile=c.rabbit_ca or None), c.rabbit_host)
        params = pika.ConnectionParameters(
            host=c.rabbit_host, port=c.rabbit_port, virtual_host=c.rabbit_vhost,
            credentials=pika.PlainCredentials(c.rabbit_user, c.rabbit_password),
            ssl_options=tls, heartbeat=30, socket_timeout=5, stack_timeout=10,
            blocked_connection_timeout=10, connection_attempts=1)
        self.conn = pika.BlockingConnection(params)
        self.channel = self.conn.channel()
        # Sem declarações ativas de exchange, filas ou bindings: responsabilidade da infraestrutura.
        self.channel.confirm_delivery()
        log.info('RabbitMQ conectado; publisher confirms ativo na exchange %s', c.exchange)

    def send(self, body, expires, risk):
        import pika
        if expires > 0:
            remaining = expires - time.time()
            if remaining <= 0:
                return False
            expiration_ms = str(max(1, int(remaining * 1000)))
        else:
            expiration_ms = None

        event_key = body.get('eventKey') or body.get('event_id')
        event_type = body.get('type') or body.get('tipo')

        props = {
            'content_type': 'application/json',
            'content_encoding': 'utf-8',
            'delivery_mode': 2 if risk else 1,
            'message_id': event_key,
            'type': event_type,
            'timestamp': int(time.time()),
        }
        if expiration_ms is not None:
            props['expiration'] = expiration_ms

        publish_started = time.perf_counter()
        self.channel.basic_publish(
            exchange=self.cfg.exchange,
            routing_key=self.cfg.alert_key if risk else self.cfg.heartbeat_key,
            body=json.dumps(body, ensure_ascii=False).encode(),
            mandatory=True,
            properties=pika.BasicProperties(**props))
        log_event(log, 'amqp.publish_confirmed', event_ref=reference(event_key), risk=risk,
                  publish_ms=round((time.perf_counter()-publish_started)*1000, 3))
        return True

    def close_connection(self):
        try:
            if self.conn and self.conn.is_open:
                self.conn.close()
        except Exception:
            pass
        self.conn = self.channel = None

    def run(self):
        try:
            while not self.stop_event.is_set():
                try:
                    if self.conn is None or self.conn.is_closed:
                        self.connect()
                    with self.hb_lock:
                        hb = self.pending_hb
                    if hb:
                        hb_body, hb_expires = hb
                        adapted_hb = adapt_legacy_payload(hb_body)
                        sent_hb = self.send(adapted_hb, hb_expires, risk=False)
                        if sent_hb:
                            log.debug('Heartbeat publicado camera=%s', adapted_hb.get('cameraCode'))
                        with self.hb_lock:
                            if self.pending_hb is hb:
                                self.pending_hb = None
                    row = self.outbox.first()
                    if row:
                        event_id, expires, raw = row
                        raw_payload = json.loads(raw)
                        payload = adapt_legacy_payload(raw_payload)
                        sent = self.send(payload, expires, risk=True)
                        if not sent:
                            log.warning('Evento expirado antes do envio: %s', event_id)
                        else:
                            log.info('Broker confirmou evento %s (não confirma recebimento no app)', event_id)
                        self.outbox.remove(event_id)
                    self.conn.process_data_events(time_limit=.2)
                except ValueError as exc:
                    self.fatal_error = f'PayloadInvalido: {type(exc).__name__}'
                    log.error('Erro irrecuperável ao converter evento da outbox (%s); interrompendo sem remover dados',
                              type(exc).__name__)
                    self.stop_event.set()
                    if self.main_stopping:
                        self.main_stopping.set()
                    break
                except Exception as exc:
                    exc_name = type(exc).__name__
                    if exc_name in ('ProbableAuthenticationError', 'ProbableAccessDeniedError'):
                        self.fatal_error = f'BrokerAuthRecusada: {exc_name}'
                        log.error('Erro irrecuperável de autenticação ou permissão no broker (%s); interrompendo publicador',
                                  exc_name)
                        self.stop_event.set()
                        if self.main_stopping:
                            self.main_stopping.set()
                        break
                    # Falhas transitórias (rede, timeout, queda de conexão): segue fluxo de reconexão
                    log_event(log, 'amqp.publish_failure', level=logging.ERROR, error_type=exc_name,
                              pending_preserved=True)
                    self.close_connection()
                    self.stop_event.wait(3)
        except Exception as unhandled:
            self.fatal_error = f'FatalUnhandled: {type(unhandled).__name__}'
            log.error('Falha fatal e inesperada na thread de publicação (%s)', type(unhandled).__name__)
            self.stop_event.set()
            if self.main_stopping:
                self.main_stopping.set()
        finally:
            self.close_connection()
