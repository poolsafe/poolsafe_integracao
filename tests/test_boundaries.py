import ast
import hashlib
import io
import json
import logging
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from config import Config
from engine_adapter import (MotorV4, V4InputError, V4ResponseError, V4ExecutionError,
                            ModelCompatibilityError, validate_model_classes, detections_from_result)
from healthcheck import healthy
from observabilidade import JsonFormatter, Observer, log_event
from transporte import Outbox
from runtime_support import configure_runtime


class BoundaryTest(unittest.TestCase):
    def motor(self):
        return MotorV4(Config())

    def test_input_error_does_not_mutate_engine(self):
        motor = self.motor()
        motor.engine.step = Mock()
        with self.assertRaises(V4InputError):
            motor.update_detections([dict(track_id=1, classe='pessoa_queda', confianca=.9, bbox_xyxy=[0])], 1, 0)
        motor.engine.step.assert_not_called()
        self.assertFalse(motor.failed)

    def test_bad_response_invalidates_instance(self):
        motor = self.motor()
        motor.engine.step = Mock(return_value=None)
        with self.assertRaises(V4ResponseError):
            motor.update_detections([], 1, 0)
        self.assertTrue(motor.failed)
        with self.assertRaises(V4ExecutionError):
            motor.update_detections([], 2, .1)

    def test_engine_exception_is_not_normal(self):
        motor = self.motor()
        motor.engine.step = Mock(side_effect=RuntimeError('fixture'))
        with self.assertRaises(V4ExecutionError):
            motor.update_detections([], 1, 0)
        self.assertEqual(motor.rows, [])

    def test_model_requires_exact_three_names(self):
        for names in ({0: 'pessoa_normal', 1: 'pessoa_queda'},
                      {0: 'pessoa_normal', 1: 'pessoa_normal', 2: 'pessoa_queda'},
                      {0: 'pessoa_normal', 1: 'pessoa_queda', 3: 'pessoa_desequilibrio'}):
            with self.assertRaises(ModelCompatibilityError):
                validate_model_classes(names)
        self.assertEqual(validate_model_classes(['pessoa_queda', 'pessoa_normal', 'pessoa_desequilibrio'])[0], 'pessoa_queda')

    def test_tensor_lengths_are_not_truncated(self):
        class Tensor:
            def __init__(self, values): self.values = values
            def cpu(self): return self
            def tolist(self): return self.values
        class Boxes:
            xyxy, cls, conf, id = Tensor([[0, 0, 1, 1]]), Tensor([]), Tensor([.9]), None
            def __len__(self): return 1
        with self.assertRaises(V4InputError):
            detections_from_result(SimpleNamespace(boxes=Boxes()), {0: 'pessoa_queda'})

    def test_reference_classes_are_unchanged(self):
        root = Path(__file__).resolve().parents[1]
        source = (root/'reference/v4_colab.py.txt').read_text()
        provenance = json.loads((root/'reference/provenance.json').read_text())
        self.assertEqual(hashlib.sha256(source.encode()).hexdigest(), provenance['sha256'])
        original = ast.parse(source)
        extracted = ast.parse((root/'motor_temporal_v4.py').read_text())
        for name in ('MotorTemporal', 'MotorTemporalContinuo'):
            a = next(n for n in original.body if isinstance(n, ast.ClassDef) and n.name == name)
            b = next(n for n in extracted.body if isinstance(n, ast.ClassDef) and n.name == name)
            self.assertEqual(ast.dump(a), ast.dump(b))

    def test_json_logs_redact_credentials_and_uuid(self):
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(JsonFormatter())
        logger = logging.getLogger('test_json_private')
        logger.handlers = [handler]
        logger.propagate = False
        logger.setLevel(logging.INFO)
        log_event(logger, 'v4.response', password='fixture-secret',
                  evidence={'base64': 'fixture-image'},
                  message='rtsp://fixture-user:fixture-password@camera/stream?token=fixture-token',
                  run_id='00000000-0000-0000-0000-000000000001')
        output = stream.getvalue()
        body = json.loads(output)
        self.assertEqual(body['event'], 'v4.response')
        for secret in ('fixture-secret', 'fixture-image', 'fixture-password', 'fixture-token', '00000000-0000-0000-0000-000000000001'):
            self.assertNotIn(secret, output)

    def test_metrics_have_bounded_buffers_and_health(self):
        cfg = Config()
        cfg.metrics_window = 3
        cfg.source_mode = 'stream'
        with tempfile.TemporaryDirectory() as directory:
            cfg.data_dir = directory
            observer = Observer(cfg, 'fixture-run')
            outbox = Outbox(Path(directory)/'outbox.sqlite3')
            observer.captured()
            for i in range(10):
                observer.completed(i*2+1, .01, .001, .015, .02)
            observer.flush(outbox, force=True)
            self.assertTrue(all(len(values) == 3 for values in observer.samples.values()))
            self.assertTrue(healthy(Path(directory)/'health.json', 90))
            body = json.loads((Path(directory)/'health.json').read_text())
            self.assertEqual(body['capture_skipped'], 9)
            self.assertEqual(body['v4_ms']['p95'], 1)
            observer.stopped('fixture')
            self.assertFalse(healthy(Path(directory)/'health.json', 90))

    def test_stale_and_missing_health(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'health.json'
            self.assertFalse(healthy(path, 90))
            path.write_text(json.dumps({'updated_at': time.time()-100, 'status': 'ready'}))
            self.assertFalse(healthy(path, 90))

    def test_cpu_threads_and_cuda_failure(self):
        cfg = Config()
        cv2 = SimpleNamespace(setNumThreads=Mock())
        torch = SimpleNamespace(set_num_threads=Mock(), set_num_interop_threads=Mock(),
                                cuda=SimpleNamespace(is_available=lambda: False))
        configure_runtime(cfg, cv2, torch)
        torch.set_num_threads.assert_called_once_with(cfg.torch_threads)
        cfg.device = '0'
        with self.assertRaises(RuntimeError):
            configure_runtime(cfg, cv2, torch)
