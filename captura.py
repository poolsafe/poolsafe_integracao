"""Captura com reconexão e um único slot de frame para streams.

O slot evita uma fila crescente no Python: o consumidor recebe o frame mais
recente decodificado. Não remove necessariamente buffers da câmera/FFmpeg/rede.
Os timeouts do OpenCV dependem do backend instalado; validar no servidor real.
"""
import logging
import threading
import time
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


class StreamReader(threading.Thread):
    def __init__(self, source, stopping, on_frame_decoded=None):
        super().__init__(name='captura', daemon=True)
        self.source, self.stopping = source, stopping
        self.on_frame_decoded = on_frame_decoded
        self.lock = threading.Lock()
        self.latest = None

    def take(self):
        with self.lock:
            result, self.latest = self.latest, None
        return result

    def run(self):
        import cv2
        generation, sequence = 0, 0
        while not self.stopping.is_set():
            cap = None
            try:
                cap = cv2.VideoCapture(self.source, cv2.CAP_FFMPEG, [
                    cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 5000,
                    cv2.CAP_PROP_READ_TIMEOUT_MSEC, 5000])
                if not cap.isOpened():
                    log.warning('Stream não abriu; nova tentativa em 3 segundos')
                else:
                    generation += 1
                    while not self.stopping.is_set():
                        ok, frame = cap.read()
                        if not ok or frame is None:
                            log.warning('Captura interrompida; reconectando')
                            break
                        sequence += 1
                        now_mono = time.monotonic()
                        now_utc = utc_now()
                        now_epoch = time.time()
                        with self.lock:
                            self.latest = (sequence, generation, frame, now_mono, now_utc)
                        if self.on_frame_decoded is not None:
                            try:
                                self.on_frame_decoded(now_utc, now_epoch)
                            except Exception as exc:
                                log.error('Erro no callback de captura: %s', type(exc).__name__)
            except Exception as exc:
                log.error('Falha de captura: %s', type(exc).__name__)
            finally:
                if cap is not None:
                    cap.release()
            self.stopping.wait(3)
