"""Outbox durável + publicação numa thread exclusiva.

Pika BlockingConnection não é compartilhada entre threads. A inferência apenas
insere no SQLite. A thread publica, aguarda publisher confirm e só então remove.
Se o processo cair entre confirm e remoção, a mensagem pode se repetir: o n8n
DEVE deduplicar por event_id. Isto é entrega com possibilidade de repetição.
"""
import json
import logging
import sqlite3
import ssl
import threading
import time
from pathlib import Path
from contextlib import contextmanager

log = logging.getLogger(__name__)


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
        with self.lock, self.connection() as db:
            if db.execute('SELECT COUNT(*) FROM pendentes').fetchone()[0] >= self.max_pending:
                # Não apagar eventos antigos silenciosamente quando faltar capacidade.
                raise RuntimeError('Outbox cheia: verificar broker e disco antes de retomar')
            db.execute('INSERT INTO pendentes VALUES (?, ?, ?)',
                       (body['event_id'], expires, json.dumps(body, ensure_ascii=False)))

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
    def __init__(self, cfg, outbox):
        super().__init__(name='rabbit-publisher', daemon=True)
        self.cfg, self.outbox = cfg, outbox
        self.stop_event = threading.Event()
        self.hb_lock = threading.Lock()
        self.pending_hb = None
        self.conn = self.channel = None

    def heartbeat(self, body, expires):
        # Só o heartbeat mais recente fica na memória. Heartbeat não é fila histórica.
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
        self.channel.exchange_declare(exchange=c.exchange, exchange_type='topic', durable=True)
        self.channel.queue_declare(queue=c.alert_queue, durable=True)
        self.channel.queue_bind(queue=c.alert_queue, exchange=c.exchange, routing_key=c.alert_key)
        self.channel.queue_declare(queue=c.heartbeat_queue, durable=True,
                                   arguments={'x-message-ttl': int(c.heartbeat_ttl * 1000)})
        self.channel.queue_bind(queue=c.heartbeat_queue, exchange=c.exchange, routing_key=c.heartbeat_key)
        self.channel.confirm_delivery()
        log.info('RabbitMQ conectado; exchange Topic e bindings conferidos')

    def send(self, body, expires, risk):
        import pika
        remaining = expires - time.time()
        if remaining <= 0:
            return False
        self.channel.basic_publish(
            exchange=self.cfg.exchange,
            routing_key=self.cfg.alert_key if risk else self.cfg.heartbeat_key,
            body=json.dumps(body, ensure_ascii=False).encode(), mandatory=True,
            properties=pika.BasicProperties(
                content_type='application/json', content_encoding='utf-8',
                delivery_mode=2 if risk else 1, message_id=body['event_id'],
                type=body['tipo'], timestamp=int(time.time()),
                expiration=str(max(1, int(remaining * 1000)))))
        return True

    def close_connection(self):
        try:
            if self.conn and self.conn.is_open:
                self.conn.close()
        except Exception:
            pass
        self.conn = self.channel = None

    def run(self):
        while not self.stop_event.is_set():
            try:
                if self.conn is None or self.conn.is_closed:
                    self.connect()
                # Heartbeat tem prioridade para não ficar atrás de um backlog de alertas.
                with self.hb_lock:
                    hb = self.pending_hb
                if hb:
                    self.send(*hb, risk=False)
                    with self.hb_lock:
                        if self.pending_hb is hb:
                            self.pending_hb = None
                row = self.outbox.first()
                if row:
                    event_id, expires, raw = row
                    sent = self.send(json.loads(raw), expires, risk=True)
                    if not sent:
                        log.warning('Evento expirado antes do envio: %s', event_id)
                    else:
                        log.info('Broker confirmou evento %s (não confirma recebimento no app)', event_id)
                    self.outbox.remove(event_id)
                self.conn.process_data_events(time_limit=.2)
            except Exception as exc:
                # O texto bruto da exceção pode conter credenciais/endereço do broker.
                log.error('Publicação falhou: %s; pendentes preservados', type(exc).__name__)
                self.close_connection()
                self.stop_event.wait(3)
        self.close_connection()
