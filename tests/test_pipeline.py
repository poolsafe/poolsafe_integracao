"""Ensaio de orquestração com captura/modelo simulados, sem inferência real."""
import importlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main as worker
from config import Config
from transporte import Outbox


class Tensor:
    def __init__(self, values):
        self.values = values
    def cpu(self):
        return self
    def tolist(self):
        return self.values


class Boxes:
    xyxy = Tensor([[10, 20, 100, 200]])
    cls = Tensor([1])
    conf = Tensor([.9])
    id = Tensor([42])
    def __len__(self):
        return 1


class FakeModel:
    names = {0: 'pessoa_normal', 1: 'pessoa_queda'}
    def track(self, *a, **kw):
        return [SimpleNamespace(boxes=Boxes())]


class FakeCapture:
    def __init__(self):
        self.left = 5
        self.closed = False
    def isOpened(self):
        return True
    def get(self, prop):
        return 1000
    def read(self):
        self.left -= 1
        return (True, object()) if self.left >= 0 else (False, None)
    def release(self):
        self.closed = True


class FakePublisher:
    def __init__(self, cfg, outbox):
        self.hb = []
        self.stop_event = SimpleNamespace(set=lambda: None)
    def start(self):
        pass
    def heartbeat(self, *a):
        raise AssertionError('Vídeo não pode emitir heartbeat')
    def is_alive(self):
        return False
    def join(self, **kw):
        pass


class PipelineTest(unittest.TestCase):
    def test_video_gera_evento_de_teste_com_modelo_e_outbox(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p/'best.pt').write_bytes(b'modelo-falso-para-teste')
            (p/'teste.mp4').write_bytes(b'captura-simulada')
            c = Config()
            c.model_path = str(p/'best.pt')
            c.source = str(p/'teste.mp4')
            c.source_mode, c.data_dir, c.test = 'video', d, True
            c.publish, c.rabbit_user, c.rabbit_password = True, 'fake', 'fake'
            c.evidence, c.frames = False, 3
            cap = FakeCapture()
            cv2 = SimpleNamespace(VideoCapture=lambda *_: cap, CAP_PROP_FPS=5)
            ultra = SimpleNamespace(YOLO=lambda *_: FakeModel())
            with patch.dict(sys.modules, {'cv2': cv2, 'ultralytics': ultra}), \
                 patch.object(worker, 'Config', return_value=c), \
                 patch.object(worker, 'Publisher', FakePublisher), \
                 patch.object(worker.signal, 'signal'):
                worker.main()
            row = Outbox(p/'outbox.sqlite3').first()
            event = json.loads(row[2])
            self.assertTrue(event['teste'])
            self.assertEqual(event['origem'], 'video')
            self.assertEqual(event['deteccao']['track_id'], 42)
            self.assertEqual(event['frame_processado'], 3)
            self.assertEqual(len(event['modelo']['sha256']), 64)
            self.assertEqual(Outbox(p/'outbox.sqlite3').count(), 1)
            self.assertTrue(cap.closed)


if __name__ == '__main__':
    unittest.main()
