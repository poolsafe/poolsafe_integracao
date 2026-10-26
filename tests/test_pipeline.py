"""Ensaio de orquestração com captura/modelo simulados, sem inferência real."""
import importlib
import json
from pathlib import Path
import sys
import tempfile
import threading
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


class FakeModelMulti:
    def __init__(self, sequence_classes=None):
        self.names = {0: 'pessoa_desequilibrio', 1: 'pessoa_normal', 2: 'pessoa_queda'}
        self.sequence_classes = sequence_classes or [0]
        self.call_count = 0

    def track(self, *a, **kw):
        idx = min(self.call_count, len(self.sequence_classes) - 1)
        cls_id = self.sequence_classes[idx]
        self.call_count += 1
        class DynamicBoxes:
            xyxy = Tensor([[10, 20, 100, 200]])
            cls = Tensor([cls_id])
            conf = Tensor([0.88])
            id = Tensor([42])
            def __len__(self):
                return 1
        return [SimpleNamespace(boxes=DynamicBoxes())]


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
    def __init__(self, cfg, outbox, stopping=None):
        self.hb = []
        self.stop_event = SimpleNamespace(set=lambda: None)
        self.fatal_error = None
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
            c.allow_test_publish = True
            c.codigo_integracao = 'cam_integracao_teste'
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
            self.assertEqual(event['schemaVersion'], '1')
            self.assertEqual(event['type'], 'POSSIVEL_RISCO')
            self.assertEqual(event['severity'], 'CRITICA')
            self.assertEqual(event['cameraCode'], 'cam_integracao_teste')
            self.assertTrue(event['eventKey'])
            self.assertTrue(event['detectedAt'].endswith('Z'))
            self.assertTrue(event['technicalData']['test'])
            self.assertEqual(event['technicalData']['source'], 'video')
            self.assertEqual(event['technicalData']['trackId'], 42)
            self.assertEqual(event['technicalData']['processedFrame'], 3)
            self.assertEqual(len(event['technicalData']['modelSha256']), 64)
            self.assertNotIn('evidence', event)
            self.assertEqual(Outbox(p/'outbox.sqlite3').count(), 1)
            self.assertTrue(cap.closed)

    def test_erro_irrecuperavel_publicador_encerra_main_preservando_outbox(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p/'best.pt').write_bytes(b'modelo-falso')
            (p/'teste.mp4').write_bytes(b'captura-falsa')
            c = Config()
            c.model_path = str(p/'best.pt')
            c.source = str(p/'teste.mp4')
            c.source_mode, c.data_dir, c.test = 'video', d, True
            c.publish, c.rabbit_user, c.rabbit_password = True, 'fake', 'fake'
            c.allow_test_publish = True
            c.codigo_integracao = 'cam_integracao_teste'
            c.evidence, c.frames = False, 3

            class FatalPublisher:
                def __init__(self, cfg, outbox, stopping=None):
                    self.cfg = cfg
                    self.outbox = outbox
                    self.stopping = stopping
                    self.stop_event = SimpleNamespace(set=lambda: None)
                    self.fatal_error = 'BrokerAuthRecusada: ProbableAuthenticationError'
                def start(self):
                    if self.stopping:
                        self.stopping.set()
                def is_alive(self):
                    return False
                def join(self, **kw):
                    pass

            cap = FakeCapture()
            cv2 = SimpleNamespace(VideoCapture=lambda *_: cap, CAP_PROP_FPS=5)
            ultra = SimpleNamespace(YOLO=lambda *_: FakeModel())
            with patch.dict(sys.modules, {'cv2': cv2, 'ultralytics': ultra}), \
                 patch.object(worker, 'Config', return_value=c), \
                 patch.object(worker, 'Publisher', FatalPublisher), \
                 patch.object(worker.signal, 'signal'):
                with self.assertRaises(RuntimeError) as ctx:
                    worker.main()
                self.assertIn('ProbableAuthenticationError', str(ctx.exception))
            # Outbox permanece intacta
            self.assertEqual(Outbox(p/'outbox.sqlite3').count(), 0)
            self.assertTrue(cap.closed)

    def test_chaves_distintas_entre_atencao_e_critica_e_estaveis_em_retries(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p/'best.pt').write_bytes(b'fake-weights')
            (p/'teste.mp4').write_bytes(b'fake-video')
            c = Config()
            c.model_path = str(p/'best.pt')
            c.source = str(p/'teste.mp4')
            c.source_mode, c.data_dir, c.test = 'video', d, True
            c.publish, c.rabbit_user, c.rabbit_password = True, 'fake', 'fake'
            c.allow_test_publish = True
            c.codigo_integracao = 'cam_integracao_teste'
            c.evidence, c.frames = False, 3

            cap = FakeCapture()
            cap.left = 6  # 6 frames
            cv2 = SimpleNamespace(VideoCapture=lambda *_: cap, CAP_PROP_FPS=5)
            # 3 frames de desequilibrio (cls 0) seguidos de 3 frames de queda (cls 2)
            ultra = SimpleNamespace(YOLO=lambda *_: FakeModelMulti([0, 0, 0, 2, 2, 2]))

            with patch.dict(sys.modules, {'cv2': cv2, 'ultralytics': ultra}), \
                 patch.object(worker, 'Config', return_value=c), \
                 patch.object(worker, 'Publisher', FakePublisher), \
                 patch.object(worker.signal, 'signal'):
                worker.main()

            outbox = Outbox(p/'outbox.sqlite3')
            self.assertEqual(outbox.count(), 2)

            # Primeiro evento: ATENCAO
            row1 = outbox.first()
            event1 = json.loads(row1[2])
            self.assertEqual(event1['severity'], 'ATENCAO')
            key_atencao = event1['eventKey']

            # Remove para ver o próximo na outbox
            outbox.remove(row1[0])
            row2 = outbox.first()
            event2 = json.loads(row2[2])
            self.assertEqual(event2['severity'], 'CRITICA')
            key_critica = event2['eventKey']

            # Chaves distintas entre atenção e crítica no mesmo episódio
            self.assertNotEqual(key_atencao, key_critica)
            self.assertEqual(event1['technicalData']['detectedClass'], 'pessoa_desequilibrio')
            self.assertEqual(event2['technicalData']['detectedClass'], 'pessoa_queda')

            # Estabilidade nos retries: re-leitura preserva a chave
            from transporte import adapt_legacy_payload
            self.assertEqual(adapt_legacy_payload(event2)['eventKey'], key_critica)

    def test_equivalencia_funcional_entre_modos_de_log(self):
        with tempfile.TemporaryDirectory() as d_prod, tempfile.TemporaryDirectory() as d_dev:
            # 1. Executa em PRODUCTION
            p_prod = Path(d_prod)
            (p_prod/'best.pt').write_bytes(b'weights')
            (p_prod/'teste.mp4').write_bytes(b'video')
            c_prod = Config()
            c_prod.model_path = str(p_prod/'best.pt')
            c_prod.source = str(p_prod/'teste.mp4')
            c_prod.source_mode, c_prod.data_dir, c_prod.test = 'video', d_prod, True
            c_prod.publish, c_prod.rabbit_user, c_prod.rabbit_password = True, 'fake', 'fake'
            c_prod.allow_test_publish = True
            c_prod.codigo_integracao = 'cam_01'
            c_prod.frames = 3
            c_prod.log_mode = 'production'

            cap_prod = FakeCapture()
            cap_prod.left = 4
            cv2 = SimpleNamespace(VideoCapture=lambda *_: cap_prod, CAP_PROP_FPS=5)
            ultra = SimpleNamespace(YOLO=lambda *_: FakeModelMulti([2, 2, 2, 2]))

            with patch.dict(sys.modules, {'cv2': cv2, 'ultralytics': ultra}), \
                 patch.object(worker, 'Config', return_value=c_prod), \
                 patch.object(worker, 'Publisher', FakePublisher), \
                 patch.object(worker.signal, 'signal'):
                worker.main()

            # Em production: resumo.json gerado, mas NÃO existe deteccoes.jsonl
            run_dirs_prod = list((p_prod / 'runs').iterdir())
            self.assertEqual(len(run_dirs_prod), 1)
            self.assertTrue((run_dirs_prod[0] / 'resumo.json').is_file())
            self.assertFalse((run_dirs_prod[0] / 'deteccoes.jsonl').exists())
            event_prod = json.loads(Outbox(p_prod/'outbox.sqlite3').first()[2])

            # 2. Executa em DEVELOPMENT
            p_dev = Path(d_dev)
            (p_dev/'best.pt').write_bytes(b'weights')
            (p_dev/'teste.mp4').write_bytes(b'video')
            c_dev = Config()
            c_dev.model_path = str(p_dev/'best.pt')
            c_dev.source = str(p_dev/'teste.mp4')
            c_dev.source_mode, c_dev.data_dir, c_dev.test = 'video', d_dev, True
            c_dev.publish, c_dev.rabbit_user, c_dev.rabbit_password = True, 'fake', 'fake'
            c_dev.allow_test_publish = True
            c_dev.codigo_integracao = 'cam_01'
            c_dev.frames = 3
            c_dev.log_mode = 'development'

            cap_dev = FakeCapture()
            cap_dev.left = 4
            cv2_dev = SimpleNamespace(VideoCapture=lambda *_: cap_dev, CAP_PROP_FPS=5)
            ultra_dev = SimpleNamespace(YOLO=lambda *_: FakeModelMulti([2, 2, 2, 2]))

            with patch.dict(sys.modules, {'cv2': cv2_dev, 'ultralytics': ultra_dev}), \
                 patch.object(worker, 'Config', return_value=c_dev), \
                 patch.object(worker, 'Publisher', FakePublisher), \
                 patch.object(worker.signal, 'signal'):
                worker.main()

            # Em development: resumo.json E deteccoes.jsonl presentes
            run_dirs_dev = list((p_dev / 'runs').iterdir())
            self.assertEqual(len(run_dirs_dev), 1)
            self.assertTrue((run_dirs_dev[0] / 'resumo.json').is_file())
            self.assertTrue((run_dirs_dev[0] / 'deteccoes.jsonl').is_file())
            jsonl_lines = (run_dirs_dev[0] / 'deteccoes.jsonl').read_text(encoding='utf-8').strip().splitlines()
            self.assertGreater(len(jsonl_lines), 0)

            event_dev = json.loads(Outbox(p_dev/'outbox.sqlite3').first()[2])

            # Equivalência funcional no evento gerado para outbox
            self.assertEqual(event_prod['severity'], event_dev['severity'])
            self.assertEqual(event_prod['type'], event_dev['type'])
            self.assertEqual(event_prod['confidence'], event_dev['confidence'])
            self.assertEqual(event_prod['cameraCode'], event_dev['cameraCode'])
            self.assertEqual(event_prod['technicalData']['detectedClass'], event_dev['technicalData']['detectedClass'])

    def test_resumo_sem_imagem_ou_credenciais(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p/'best.pt').write_bytes(b'weights')
            c = Config()
            c.model_path = str(p/'best.pt')
            c.source = 'rtsp://usuario:senha_ultra_secreta@10.0.0.1:554/live'
            c.source_mode, c.data_dir, c.test = 'stream', d, True
            c.publish = False
            c.frames = 3

            ultra = SimpleNamespace(YOLO=lambda *_: FakeModelMulti([1]))
            stopping = threading.Event()

            class FakeReader:
                def __init__(self, source, stopping, on_frame_decoded=None):
                    self.stopping = stopping
                def start(self):
                    pass
                def take(self):
                    self.stopping.set()
                    return None
                def join(self, **kw):
                    pass

            fake_cv2 = SimpleNamespace()
            with patch.object(worker, 'StreamReader', FakeReader), \
                 patch.dict(sys.modules, {'cv2': fake_cv2, 'ultralytics': ultra}), \
                 patch.object(worker, 'Config', return_value=c), \
                 patch.object(worker.signal, 'signal'):
                worker.main()

            run_dirs = list((p / 'runs').iterdir())
            self.assertEqual(len(run_dirs), 1)
            resumo_path = run_dirs[0] / 'resumo.json'
            self.assertTrue(resumo_path.is_file())
            resumo_text = resumo_path.read_text(encoding='utf-8')
            resumo_json = json.loads(resumo_text)

            # Senhas nunca devem estar no resumo
            self.assertNotIn('senha_ultra_secreta', resumo_text)
            self.assertIn('rtsp://usuario:***@10.0.0.1:554/live', resumo_text)
            # Imagem / base64 nunca deve estar no resumo
            self.assertNotIn('base64', resumo_text)
            # Em modo stream, métricas de arquivo de vídeo são None (null), diferenciam de 0
            self.assertIsNone(resumo_json['video'])


if __name__ == '__main__':
    unittest.main()

