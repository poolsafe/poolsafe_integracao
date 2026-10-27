import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import threading
import time
import traceback
from collections import deque
from datetime import datetime, timezone

_UUID = re.compile(r'\b[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\b')
_URL = re.compile(r'(?:rtsp|rtsps|http|https|amqp|amqps)://[^\s\"<>]+', re.I)
_SECRET = re.compile(r'(?i)((?:password|passwd|token|secret|authorization|service[_-]?key|api[_-]?key)\s*[=:]\s*)[^\s,;]+')
_SENSITIVE_KEYS = {'password', 'token', 'secret', 'authorization', 'base64', 'evidence', 'source', 'rabbit_password', 'fcmToken'}


def reference(value):
    return hashlib.sha256(str(value).encode()).hexdigest()[:16]


def sanitize(value, key='', depth=0):
    if key.lower() in {k.lower() for k in _SENSITIVE_KEYS}:
        return '[redacted]'
    if depth > 8:
        return '[depth-limit]'
    if isinstance(value, dict):
        return {str(k): sanitize(v, str(k), depth+1) for k, v in list(value.items())[:100]}
    if isinstance(value, (list, tuple)):
        return [sanitize(v, '', depth+1) for v in value[:100]]
    if isinstance(value, str):
        value = _URL.sub('[url-redacted]', value)
        value = _SECRET.sub(r'\1[redacted]', value)
        return _UUID.sub(lambda m: 'ref:'+reference(m.group()), value)[:4000]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None or isinstance(value, (int, float, bool)):
        return value
    return sanitize(str(value), '', depth+1)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        output = {
            'timestamp': datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
            'level': record.levelname, 'service': 'poolsafe-worker',
            'logger': record.name, 'event': getattr(record, 'event_name', 'service.message'),
            'message': sanitize(record.getMessage()),
        }
        output.update(sanitize(getattr(record, 'fields', {})))
        if record.exc_info:
            output['error_type'] = record.exc_info[0].__name__
            output['stack'] = sanitize(''.join(traceback.format_tb(record.exc_info[2])))
        return json.dumps(output, ensure_ascii=False, allow_nan=False)


def log_event(logger, event, level=logging.INFO, **fields):
    logger.log(level, event, extra={'event_name': event, 'fields': fields})


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[min(len(ordered)-1, math.ceil(len(ordered)*p)-1)], 3)


class Observer:
    def __init__(self, cfg, run_id):
        self.cfg, self.run_id = cfg, run_id
        self.log = logging.getLogger('poolsafe.metrics')
        self.started = time.monotonic()
        self.last_flush = float('-inf')
        self.last_capture = self.last_inference = None
        self.frames = 0
        self.previous_sequence = None
        self.skipped = 0
        self.lock = threading.Lock()
        self.samples = {key: deque(maxlen=cfg.metrics_window) for key in ('inference_ms', 'v4_ms', 'pipeline_ms', 'frame_age_ms')}

    def captured(self):
        with self.lock:
            self.last_capture = time.monotonic()

    def completed(self, sequence, inference_s, v4_s, pipeline_s, frame_age_s):
        now = time.monotonic()
        with self.lock:
            if self.previous_sequence is not None:
                self.skipped += max(0, sequence-self.previous_sequence-1)
            self.previous_sequence = sequence
            self.frames += 1
            self.last_inference = now
            for key, value in zip(self.samples, (inference_s, v4_s, pipeline_s, frame_age_s)):
                self.samples[key].append(max(0, value)*1000)

    def flush(self, outbox, force=False):
        now = time.monotonic()
        if not force and now-self.last_flush < self.cfg.metrics_interval:
            return
        self.last_flush = now
        outbox_count = outbox.count()
        with self.lock:
            capture_age = None if self.last_capture is None else now-self.last_capture
            inference_age = None if self.last_inference is None else now-self.last_inference
            payload = dict(run_ref=reference(self.run_id), camera_ref=reference(self.cfg.camera_id),
                           engine='v4', frames=self.frames, capture_skipped=self.skipped,
                           fps_effective=round(self.frames/max(.001, now-self.started), 3),
                           capture_age_s=capture_age, inference_age_s=inference_age,
                           outbox_pending=outbox_count)
            for key, values in self.samples.items():
                payload[key] = dict(p50=percentile(values, .5), p95=percentile(values, .95), max=round(max(values), 3) if values else None)
        first = outbox.first()
        if first:
            try:
                body = json.loads(first[2])
                detected = datetime.fromisoformat(body['detectedAt'].replace('Z', '+00:00')).timestamp()
                payload['outbox_oldest_age_s'] = round(max(0, time.time()-detected), 3)
            except (ValueError, TypeError, KeyError):
                payload['outbox_oldest_age_s'] = None
        log_event(self.log, 'worker.metrics', **payload)
        stale = inference_age is None or inference_age > self.cfg.health_stale
        if self.cfg.source_mode == 'stream':
            stale = stale or capture_age is None or capture_age > self.cfg.health_stale
        health = dict(updated_at=time.time(), status='degraded' if stale else 'ready', **payload)
        path = Path(self.cfg.data_dir) / 'health.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix('.tmp')
        temporary.write_text(json.dumps(health, ensure_ascii=False, allow_nan=False), encoding='utf-8')
        os.replace(temporary, path)
        if payload['frames'] and payload['fps_effective'] < self.cfg.engine_fps:
            log_event(self.log, 'worker.sampling_below_reference', level=logging.WARNING,
                      fps_effective=payload['fps_effective'], reference_fps=self.cfg.engine_fps)
        if stale:
            log_event(self.log, 'worker.degraded', level=logging.WARNING,
                      capture_age_s=capture_age, inference_age_s=inference_age)

    def stopped(self, reason):
        path = Path(self.cfg.data_dir) / 'health.json'
        path.write_text(json.dumps({'updated_at': time.time(), 'status': 'stopped', 'reason': sanitize(reason)}), encoding='utf-8')
