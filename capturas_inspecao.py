"""Capturas locais opcionais: uma cópia, nenhuma inferência ou mudança de evidência."""
import copy
import heapq
import json
import logging
from pathlib import Path
import queue
import threading

from observabilidade import log_event

log = logging.getLogger('poolsafe.capturas')


class CaptureQueue(queue.PriorityQueue):
    def put_capture(self, item):
        """Evento pode substituir uma amostra pendente, sem bloquear o motor."""
        evicted = False
        with self.not_full:
            if self._qsize() >= self.maxsize:
                sample = next((i for i, pending in enumerate(self.queue) if pending[0] == 1), None)
                if item[0] != 0 or sample is None:
                    raise queue.Full
                del self.queue[sample]
                heapq.heapify(self.queue)
                self.unfinished_tasks -= 1
                evicted = True
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()
        return evicted


class CaptureWriter:
    # Fila limitada para não acumular imagens quando o disco estiver lento.
    def __init__(self, cfg, run_dir, run_id):
        self.cfg = cfg
        self.directory = Path(run_dir) / 'capturas'
        self.run_id = run_id
        self.last_sample = None
        self.count = self.bytes = 0
        self.suspended = False
        self.warned = set()
        self.queue = CaptureQueue(maxsize=32)
        self.sequence = 0
        self.thread = None
        if cfg.captures_enabled:
            self.thread = threading.Thread(target=self._run, name='inspection-captures', daemon=True)
            self.thread.start()

    def _warn(self, code, exc=None):
        if code not in self.warned:
            self.warned.add(code)
            log_event(log, 'capture.warning', level=logging.WARNING, code=code,
                      error_type=type(exc).__name__ if exc else None)

    def submit(self, frame, detections, rows, *, events, segment, processed_frame,
               captured_frame, observed_at, video_time_s, sample_time):
        if not self.thread or self.suspended:
            return
        # Usa a aceitação já calculada pelo motor, sem repetir seus limiares.
        accepted = [d for d, row in zip(detections, rows) if row['aceita']]
        if not events and (not accepted or (self.last_sample is not None and
                                            sample_time-self.last_sample < self.cfg.captures_interval)):
            return
        try:
            metadata = dict(run=self.run_id, segment=segment, processed_frame=processed_frame,
                            captured_frame=captured_frame, observed_at=observed_at,
                            video_time_s=video_time_s, boxes=copy.deepcopy(accepted),
                            events=copy.deepcopy(events))
            self.sequence += 1
            evicted = self.queue.put_capture((0 if events else 1, self.sequence, frame.copy(), metadata))
            if evicted:
                self._warn('sample_evicted_for_event')
            # Eventos também evitam amostras repetidas imediatamente após o alerta.
            self.last_sample = sample_time
        except queue.Full:
            self._warn('queue_full_event' if events else 'queue_full_sample')
        except Exception as exc:
            self._warn('enqueue_failed', exc)

    def _run(self):
        while True:
            _, _, frame, metadata = self.queue.get()
            try:
                if metadata is None:
                    return
                if not self.suspended:
                    self._save(frame, metadata)
            except Exception as exc:
                self._warn('write_failed', exc)
            finally:
                self.queue.task_done()

    def _save(self, frame, metadata):
        import cv2
        height, width = frame.shape[:2]
        boxes = metadata['boxes']
        # A imagem é uma cópia privada da thread. Coordenadas indexadas são as
        # mesmas usadas para desenhar, limitadas à resolução original salva.
        for d in boxes:
            x1, y1, x2, y2 = d['bbox_xyxy']
            bounds = [max(0, min(width-1, round(x1))), max(0, min(height-1, round(y1))),
                      max(0, min(width-1, round(x2))), max(0, min(height-1, round(y2)))]
            d['bbox_xyxy'] = bounds
            cv2.rectangle(frame, tuple(bounds[:2]), tuple(bounds[2:]), (0, 255, 255), 2)
            label = f"ID={d['track_id']} {d['classe']} {d['confianca']:.3f}"
            scale = .45
            text_width = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, scale, 1)[0][0]
            if text_width > width-4:
                scale *= max(1, width-4)/text_width
            text_width, text_height = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, scale, 1)[0]
            origin = (max(0, min(bounds[0], width-text_width-1)),
                      min(height-1, max(text_height+1, bounds[1]-5)))
            cv2.putText(frame, label, origin, cv2.FONT_HERSHEY_SIMPLEX, scale, (0, 255, 255), 1)
        ok, encoded = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
        if not ok or encoded is None:
            raise ValueError('jpeg_encode_failed')
        raw = encoded.tobytes()
        events = metadata.pop('events')
        for event in events or [None]:
            kind = 'evento' if event else 'amostra'
            suffix = event['event_ref'] if event else 'sem_evento'
            name = f"{kind}_s{metadata['segment']}_f{metadata['processed_frame']}_{suffix}.jpg"
            record = dict(metadata, kind=kind, event=event, event_ref=event['event_ref'] if event else None,
                          path=f'capturas/{name}', width=width, height=height)
            line = (json.dumps(record, ensure_ascii=False, allow_nan=False)+'\n').encode('utf-8')
            required = len(raw)+len(line)
            if self.count >= self.cfg.captures_max_count or self.bytes+required > self.cfg.captures_max_bytes:
                self.suspended = True
                self._warn('storage_limit_reached')
                return
            self.directory.mkdir(parents=True, exist_ok=True)
            path = self.directory / name
            # Contabiliza também arquivos órfãos em caso de falha no índice.
            try:
                path.write_bytes(raw)
            finally:
                if path.exists():
                    self.count += 1
                    self.bytes += path.stat().st_size
            with (self.directory / 'indice.jsonl').open('ab') as index:
                index.write(line)
            self.bytes += len(line)

    def close(self):
        if self.thread:
            self.queue.join()
            self.queue.put((2, self.sequence+1, None, None))
            self.thread.join()
