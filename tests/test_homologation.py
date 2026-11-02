import json
import logging
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from capturas_inspecao import CaptureWriter, CaptureQueue
from config import Config
from engine_adapter import MotorV4
from observabilidade import JsonFormatter, Observer, log_risk_event
from transporte import Outbox

try:
    import cv2
    import numpy as np
except ImportError:
    cv2 = np = None


def detection():
    return dict(track_id=7, classe='pessoa_queda', confianca=.9, bbox_xyxy=[-2, 20, 400, 100])


class HomologationLogsTest(unittest.TestCase):
    def test_invalid_observability_settings_rejected(self):
        for fields in (dict(homologation_log_every=0), dict(captures_interval=float('nan')),
                       dict(captures_interval=0), dict(captures_max_count=-1), dict(captures_max_bytes=0)):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                Config(**fields).validate()

    def test_event_evicts_sample_and_queue_remains_bounded(self):
        import queue
        pending = CaptureQueue(maxsize=2)
        pending.put_capture((1, 1, 'sample'))
        pending.put_capture((0, 2, 'event'))
        self.assertTrue(pending.put_capture((0, 3, 'event2')))
        with self.assertRaises(queue.Full):
            pending.put_capture((0, 4, 'event3'))
        self.assertEqual(pending.qsize(), 2)
        self.assertEqual(pending.get()[2], 'event')
        pending.task_done()
        self.assertEqual(pending.get()[2], 'event2')
        pending.task_done()
        pending.join()

    def test_frame_period_and_no_duplicate_metrics(self):
        for every in (100, 7):
            with self.subTest(every=every), tempfile.TemporaryDirectory() as directory:
                cfg = Config(log_mode='homologation', homologation_log_every=every, data_dir=directory)
                observer = Observer(cfg, 'run')
                outbox = Outbox(Path(directory)/'o.db')
                with self.assertLogs('poolsafe.metrics', logging.INFO) as logs:
                    for frame in range(1, every*2+2):
                        observer.captured()
                        observer.completed(frame, .02, .001, .03, .04, [detection()], frame, 2)
                        observer.flush(outbox, force=True)
                records = [r for r in logs.records if r.event_name == 'worker.homologation_summary']
                self.assertEqual([r.fields['frame'] for r in records], [every, every*2])
                self.assertFalse(any(r.event_name == 'worker.metrics' for r in logs.records))
                fields = records[0].fields
                self.assertEqual(fields['track_ids'], [7])
                self.assertEqual(fields['interval_detections_by_class'], {'pessoa_queda': every})
                self.assertEqual(fields['inference_ms']['p95'], 20)
                self.assertEqual(fields['frame_age_ms']['max'], 40)
                health = json.loads((Path(directory)/'health.json').read_text())
                self.assertEqual(health['frames'], every*2+1)

    def test_immediate_highlights_and_structured_fields(self):
        logger = logging.getLogger('test.homologation')
        for severity, badge in [('ATENCAO', '🟡 ATENCAO'), ('CRITICA', '🔴 CRITICA')]:
            with self.assertLogs(logger, logging.INFO) as logs:
                log_risk_event(logger, 'homologation', severity=severity, event_ref='ref123',
                               frame=3, classe='pessoa_queda', track_id=7,
                               observed_at='2026-11-02T10:00:00Z')
            body = json.loads(JsonFormatter().format(logs.records[0]))
            self.assertEqual(body['event'], 'risk.event')
            self.assertEqual(body['severity'], severity)
            for value in (badge, 'ref123', 'track=7', 'frame=3', body['observed_at']):
                self.assertIn(value, body['message'])

    def test_defaults_and_policy_unchanged(self):
        self.assertTrue(Config(log_mode='homologation').captures_enabled)
        self.assertFalse(Config(log_mode='production').captures_enabled)
        policies, results = [], []
        for mode in ('production', 'development', 'homologation'):
            cfg = Config(log_mode=mode)
            policies.append(cfg.engine_policy())
            motor = MotorV4(cfg)
            events = []
            for frame in range(1, 120):
                ds = [detection()]
                if 20 < frame < 40:
                    ds[0]['classe'] = 'pessoa_normal'
                events += [(d.status, d.severity, d.event) for d in motor.update_detections(ds, frame, frame/15)]
            results.append(events)
        self.assertEqual(policies[0], policies[2])
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0], results[2])


@unittest.skipIf(cv2 is None, 'OpenCV/NumPy necessários para JPEG real')
class CaptureTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.cfg = Config(log_mode='homologation')
        self.frame = np.zeros((120, 160, 3), dtype=np.uint8)
        self.detection = detection()
        self.writer = CaptureWriter(self.cfg, self.temp.name, 'run-test')
        self.addCleanup(self.writer.close)

    def submit(self, number, when, events=(), accepted=True):
        self.writer.submit(self.frame, [self.detection], [{'aceita': accepted}], events=events,
                           segment=2, processed_frame=number, captured_frame=number*3,
                           observed_at='2026-11-02T10:00:00.000Z', video_time_s=when, sample_time=when)
        self.writer.queue.join()

    def index(self):
        path = Path(self.temp.name)/'capturas/indice.jsonl'
        return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()] if path.exists() else []

    def test_sample_interval_and_event_bypass_with_metadata(self):
        self.submit(1, 0)
        self.submit(2, .3)
        self.submit(3, .4, events=[dict(event_ref='abc', severity='ATENCAO')])
        self.submit(4, .5, events=[dict(event_ref='def', severity='CRITICA')])
        self.submit(5, 1)
        self.submit(6, 1.5)
        records = self.index()
        self.assertEqual([r['processed_frame'] for r in records], [1, 3, 4, 6])
        self.assertEqual([r['kind'] for r in records], ['amostra', 'evento', 'evento', 'amostra'])
        self.assertEqual(records[1]['event_ref'], 'abc')
        self.assertEqual(records[1]['captured_frame'], 9)
        self.assertEqual(records[1]['run'], 'run-test')
        self.assertEqual(records[1]['segment'], 2)
        self.assertEqual(records[1]['video_time_s'], .4)
        self.assertEqual(records[1]['observed_at'], '2026-11-02T10:00:00.000Z')
        self.assertEqual(records[0]['boxes'][0]['bbox_xyxy'], [0, 20, 159, 100])
        for record in records:
            image = cv2.imread(str(Path(self.temp.name)/record['path']))
            self.assertEqual(image.shape[:2], (record['height'], record['width']))
            self.assertGreater(int(image.sum()), 0)
        self.assertFalse(self.frame.any())
        self.assertEqual(self.detection['bbox_xyxy'], [-2, 20, 400, 100])

    def test_rejected_detections_do_not_sample(self):
        self.submit(1, 0, accepted=False)
        self.assertEqual(self.index(), [])

    def test_multiple_events_same_frame_have_separate_images(self):
        self.submit(1, 0, events=[dict(event_ref='first', severity='ATENCAO'),
                                  dict(event_ref='second', severity='CRITICA')])
        records = self.index()
        self.assertEqual([r['event_ref'] for r in records], ['first', 'second'])
        self.assertNotEqual(records[0]['path'], records[1]['path'])
        self.assertEqual(self.writer.count, 2)
        self.assertEqual(self.writer.bytes, sum(p.stat().st_size for p in (Path(self.temp.name)/'capturas').iterdir()))

    def test_encoder_false_does_not_stop_writer(self):
        with patch.object(cv2, 'imencode', return_value=(False, None)):
            self.submit(1, 0)
        self.submit(2, 2)
        self.assertEqual(len(self.index()), 1)

    def test_index_failure_counts_orphan_and_recovers(self):
        original_open = Path.open
        def fail_index(path, *args, **kwargs):
            if path.name == 'indice.jsonl':
                raise PermissionError('fixture')
            return original_open(path, *args, **kwargs)
        with patch.object(Path, 'open', fail_index):
            self.submit(1, 0)
        self.assertEqual(self.writer.count, 1)
        self.assertGreater(self.writer.bytes, 0)
        self.assertEqual(self.index(), [])
        self.submit(2, 2)
        self.assertEqual(len(self.index()), 1)

    def test_count_limit_suspends_once(self):
        self.cfg.captures_max_count = 1
        self.submit(1, 0)
        with self.assertLogs('poolsafe.capturas', logging.WARNING) as logs:
            self.submit(2, 2)
            self.submit(3, 3)
        self.assertEqual(len(self.index()), 1)
        self.assertTrue(self.writer.suspended)
        self.assertEqual(len(logs.records), 1)

    def test_byte_limit_includes_index(self):
        self.cfg.captures_max_bytes = 1
        self.submit(1, 0)
        self.assertTrue(self.writer.suspended)
        self.assertEqual(self.index(), [])
        self.assertEqual(list(Path(self.temp.name).rglob('*.jpg')), [])

    def test_encoding_failure_recovers_and_sanitizes(self):
        with patch.object(cv2, 'imencode', side_effect=RuntimeError('rtsp://private/secret')):
            with self.assertLogs('poolsafe.capturas', logging.WARNING) as logs:
                self.submit(1, 0)
        self.assertNotIn('private', JsonFormatter().format(logs.records[0]))
        self.submit(2, 2)
        self.assertEqual(len(self.index()), 1)
        self.assertFalse(self.writer.suspended)

    def test_filesystem_failure_does_not_escape(self):
        with patch.object(Path, 'mkdir', side_effect=PermissionError('private')):
            with self.assertLogs('poolsafe.capturas', logging.WARNING):
                self.submit(1, 0, events=[dict(event_ref='abc')])
        self.submit(2, 2)
        self.assertEqual(len(self.index()), 1)

    def test_worker_modes_keep_inference_decisions_evidence_and_publication(self):
        import main as worker
        from pipeline_fakes import FakeCapture, FakeModelMulti, FakePublisher
        outcomes = []
        for mode, fail_images, diagnostics, fail_audit in [
                ('production', False, False, False), ('development', False, False, False),
                ('homologation', False, False, False), ('homologation', True, False, False),
                ('homologation', False, True, False), ('homologation', False, True, True)]:
            with self.subTest(mode=mode, failure=fail_images), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root/'best.pt').write_bytes(b'fixture')
                (root/'video.mp4').write_bytes(b'fixture')
                cfg = Config(log_mode=mode, model_path=str(root/'best.pt'), source=str(root/'video.mp4'),
                             data_dir=directory, publish=True, allow_test_publish=True,
                             codigo_integracao='controlled', rabbit_user='fixture', rabbit_password='fixture',
                             evidence=True, homologation_log_every=10,
                             homologation_diagnostics_enabled=diagnostics)
                cap = FakeCapture()
                cap.left = 25
                original = np.zeros((240, 320, 3), dtype=np.uint8)
                def read():
                    cap.left -= 1
                    return (True, original) if cap.left >= 0 else (False, None)
                cap.read = read
                model = FakeModelMulti([0]*15+[2]*10)
                torch = SimpleNamespace(set_num_threads=lambda _: None, set_num_interop_threads=lambda _: None,
                                        cuda=SimpleNamespace(is_available=lambda: False))
                save = CaptureWriter._save
                def maybe_save(writer, *args):
                    if fail_images:
                        raise PermissionError('rtsp://private/credentials')
                    return save(writer, *args)
                original_open = Path.open
                def maybe_open(path, *args, **kwargs):
                    if fail_audit and path.name == 'diagnostico_frames.jsonl':
                        raise PermissionError('fixture')
                    return original_open(path, *args, **kwargs)
                with patch.object(worker, 'Config', return_value=cfg), patch.object(worker, 'Publisher', FakePublisher), \
                     patch.object(cv2, 'VideoCapture', return_value=cap), \
                     patch.object(CaptureWriter, '_save', maybe_save), \
                     patch.object(Path, 'open', maybe_open), \
                     patch('diagnostico_frames.version', return_value='8.4.173'), \
                     patch.dict(sys.modules, {'torch': torch, 'ultralytics': SimpleNamespace(YOLO=lambda _: model)}), \
                     self.assertLogs(level=logging.INFO) as logs:
                    worker.main()
                self.assertEqual(model.call_count, 25)
                self.assertFalse(original.any())
                run = root/'runs'/(root/'latest_run').read_text()
                events = [json.loads(line) for line in (run/'eventos.jsonl').read_text().splitlines()]
                diagnostic_path = run/'diagnostico_frames.jsonl'
                if diagnostics and not fail_audit:
                    records = [json.loads(line) for line in diagnostic_path.read_text(encoding='utf-8').splitlines()]
                    self.assertEqual(len(records), 25)
                    self.assertTrue(all(r['etapas']['detector_pos_nms'][0]['track_id'] is None for r in records))
                    self.assertTrue(all(r['etapas']['apos_tracker'][0]['track_id'] == 42 for r in records))
                else:
                    self.assertFalse(diagnostic_path.exists())
                outcomes.append([(e['severity'], e['classe'], e['frame_processado']) for e in events])
                outbox = Outbox(root/'outbox.sqlite3')
                for severity in ('ATENCAO', 'CRITICA'):
                    first = outbox.first()
                    body = json.loads(first[2])
                    self.assertEqual(body['severity'], severity)
                    self.assertEqual(body['evidence'], worker.prepare_evidence(original))
                    outbox.remove(first[0])
                risk = [r for r in logs.records if getattr(r, 'event_name', '') == 'risk.event']
                self.assertEqual([r.fields['frame'] for r in risk], [15, 25])
                if mode == 'homologation':
                    self.assertFalse((run/'deteccoes.jsonl').exists())
                    self.assertFalse(any(getattr(r, 'event_name', '') in ('v4.request', 'v4.response', 'worker.metrics') for r in logs.records))
                    summaries = [r.fields['frame'] for r in logs.records if getattr(r, 'event_name', '') == 'worker.homologation_summary']
                    self.assertEqual(summaries, [10, 20])
                    if not fail_images:
                        index = [json.loads(line) for line in (run/'capturas/indice.jsonl').read_text().splitlines()]
                        self.assertEqual([r['processed_frame'] for r in index if r['kind'] == 'evento'], [15, 25])
        self.assertTrue(all(result == outcomes[0] for result in outcomes))


if __name__ == '__main__':
    unittest.main()
