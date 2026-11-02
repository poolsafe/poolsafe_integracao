import copy
import json
import logging
from pathlib import Path
import tempfile
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from config import Config
from diagnostico_frames import FrameDiagnostics
from engine_adapter import MotorV4, detections_from_result
from observabilidade import Observer
from pipeline_fakes import Tensor


NAMES = {0: 'pessoa_normal', 1: 'pessoa_desequilibrio', 2: 'pessoa_queda'}


def detection(tid=7, score=.9, label='pessoa_queda'):
    return dict(track_id=tid, classe=label, confianca=score, bbox_xyxy=[10, 20, 100, 200])


def result(ds):
    class Boxes:
        def __len__(self):
            return len(ds)
    boxes = Boxes()
    boxes.xyxy = Tensor([d['bbox_xyxy'] for d in ds])
    boxes.cls = Tensor([next(k for k, v in NAMES.items() if v == d['classe']) for d in ds])
    boxes.conf = Tensor([d['confianca'] for d in ds])
    boxes.id = Tensor([d['track_id'] for d in ds]) if ds and all(d['track_id'] is not None for d in ds) else None
    return SimpleNamespace(boxes=boxes)


class CallbackModel:
    """Mimics lazy register_tracker + ordered postprocess callbacks in 8.4.173."""
    def __init__(self):
        self.callbacks = {'on_predict_postprocess_end': [lambda _: None]}
        self.calls = 0
        self.tracker = None

    def add_callback(self, event, cb):
        self.callbacks[event].append(cb)

    def track(self, ds, tracked):
        self.tracked = tracked
        if self.tracker is None:
            def tracker(predictor):
                # Mutate originals too: saved values must remain independent.
                for box in predictor.results[0].boxes.xyxy.values:
                    box[0] = -999
                predictor.results[0] = result(self.tracked)
            self.tracker = tracker
            self.add_callback('on_predict_postprocess_end', tracker)
        self.calls += 1
        predictor = SimpleNamespace(results=[result(copy.deepcopy(ds))])
        for cb in self.callbacks['on_predict_postprocess_end']:
            cb(predictor)
        return predictor.results


class FrameDiagnosticsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.cfg = Config(log_mode='homologation', homologation_diagnostics_enabled=True)
        self.writer = FrameDiagnostics(self.cfg, self.temp.name, 'run')
        self.motor = MotorV4(self.cfg)

    def step(self, frame, ds, now=None):
        before = self.writer.begin(self.motor)
        now = frame/15 if now is None else now
        decisions = self.motor.update_detections(ds, frame, now)
        self.writer.write(motor=self.motor, before=before, detections=ds, decisions=decisions,
                          segment=1, processed_frame=frame, captured_frame=frame*3,
                          observed_at='2026-11-02T10:00:00Z', logical_time=now,
                          source_mode=self.cfg.source_mode, inference_conf=.3, iou=.7)
        return decisions

    def records(self):
        return [json.loads(line) for line in self.writer.path.read_text(encoding='utf-8').splitlines()]

    def test_empty_no_id_threshold_and_incomplete_are_distinct_and_file_only(self):
        with patch('diagnostico_frames.log_event') as logger:
            self.step(1, [])
            self.step(2, [detection(None, .528)])
            self.step(3, [detection(score=.49)])
            self.step(4, [detection()])
        logger.assert_not_called()
        records = self.records()
        self.assertTrue(records[0]['sem_deteccao'])
        self.assertEqual(records[0]['observacoes'], [])
        rows = [r['observacoes'][0] for r in records[1:]]
        self.assertEqual([r['motivo'] for r in rows], ['sem_id', 'abaixo_limiar', 'aguardando_persistencia'])
        self.assertEqual([r['aceita'] for r in rows], [True, False, True])
        self.assertFalse(rows[0]['usada_temporal'])
        self.assertFalse(rows[2]['confirmacao_temporal'])
        self.assertEqual(records[1]['frame_capturado'], 6)
        self.assertEqual(records[1]['referencia_temporal']['tipo'], 'video_nominal')

    def test_absence_records_state_reset_and_retained_fall_window(self):
        self.step(1, [detection(label='pessoa_desequilibrio')])
        self.step(2, [])
        record = self.records()[-1]
        self.assertEqual(record['ids_ausentes'], ['7'])
        self.assertEqual(record['estado_antes']['7']['estado']['counts']['pessoa_desequilibrio'], 1)
        self.assertEqual(record['estado_depois']['7']['estado']['counts']['pessoa_desequilibrio'], 0)
        self.step(3, [detection()])
        self.step(4, [])
        self.assertEqual(len(self.records()[-1]['estado_depois']['7']['evidencias_queda']), 1)

    def test_limit_stops_only_collection_warns_once(self):
        self.writer.limit = 2
        with self.assertLogs('poolsafe.diagnostico', logging.WARNING) as logs:
            for frame in range(1, 16):
                self.step(frame, [detection()])
        self.assertEqual(len(logs.records), 1)
        self.assertEqual(logs.records[0].fields['code'], 'frame_limit_reached')
        self.assertEqual(len(self.records()), 2)
        self.assertTrue(self.motor.engine.states[7]['active'])

    def test_write_failure_isolated_and_no_private_exception_text(self):
        with patch.object(Path, 'open', side_effect=PermissionError('private credentials')):
            with self.assertLogs('poolsafe.diagnostico', logging.WARNING) as logs:
                for frame in range(1, 16):
                    self.step(frame, [detection()])
        self.assertEqual(len(logs.records), 1)
        self.assertNotIn('private', str(logs.records[0].fields))
        self.assertEqual(self.writer.count, 0)
        self.assertTrue(self.motor.engine.states[7]['active'])

    def test_defaults_and_mode_gate(self):
        self.assertFalse(Config().homologation_diagnostics_enabled)
        self.assertEqual(Config().homologation_diagnostics_max_frames, 600)
        for mode in ('production', 'development'):
            writer = FrameDiagnostics(Config(log_mode=mode, homologation_diagnostics_enabled=True), self.temp.name, 'run')
            self.assertFalse(writer.active)
        with self.assertRaises(ValueError):
            Config(homologation_diagnostics_max_frames=0).validate()

    def test_pre_tracker_copy_same_call_order_and_no_id_post_tracker(self):
        model = CallbackModel()
        original_callback = model.callbacks['on_predict_postprocess_end'][0]
        self.writer.ultralytics_version = '8.4.173'
        self.writer.install(model, NAMES)
        for pre, post in (([], []), ([detection(None, .528)], [detection(None, .528)]),
                          ([detection(None, .528)], [detection()])):
            self.writer.begin(self.motor)
            output = model.track(pre, post)
            self.assertEqual(self.writer.pre_tracker, pre)
            self.assertEqual(detections_from_result(output[0], NAMES), post)
            callbacks = model.callbacks['on_predict_postprocess_end']
            self.assertIs(callbacks[0], original_callback)
            self.assertIs(callbacks[-1], model.tracker)
        self.assertEqual(model.calls, 3)
        self.writer.stopped = True
        self.writer.begin(self.motor)
        model.track([detection(None)], [detection()])
        self.assertIsNone(self.writer.pre_tracker)

    def test_callback_failure_is_isolated(self):
        model = CallbackModel()
        self.writer.ultralytics_version = '8.4.173'
        self.writer.install(model, NAMES)
        with patch('diagnostico_frames.detections_from_result', side_effect=RuntimeError('fixture')):
            with self.assertLogs('poolsafe.diagnostico', logging.WARNING):
                output = model.track([detection(None)], [detection()])
        self.assertEqual(detections_from_result(output[0], NAMES), [detection()])
        self.assertIsNone(self.writer.pre_tracker)
        self.assertEqual(model.calls, 1)

    def test_existing_tracker_callback_is_not_reordered_or_replaced(self):
        model = CallbackModel()
        def tracker(_):
            pass
        tracker.__module__ = 'ultralytics.trackers.track'
        model.callbacks['on_predict_postprocess_end'].append(tracker)
        original = list(model.callbacks['on_predict_postprocess_end'])
        self.writer.ultralytics_version = '8.4.173'
        with self.assertLogs('poolsafe.diagnostico', logging.WARNING):
            self.writer.install(model, NAMES)
        self.assertEqual(model.callbacks['on_predict_postprocess_end'], original)
        self.assertEqual(self.writer.stage_status, 'indisponivel_tracker_ja_registrado')

    def test_unavailable_hook_does_not_fabricate_pre_tracker(self):
        self.writer.ultralytics_version = 'unknown'
        model = CallbackModel()
        with self.assertLogs('poolsafe.diagnostico', logging.WARNING):
            self.writer.install(model, NAMES)
        self.step(1, [detection()])
        record = self.records()[0]
        self.assertIsNone(record['etapas']['detector_pos_nms'])
        self.assertEqual(record['etapas']['status'], 'indisponivel_versao_nao_validada')
        self.assertEqual(len(model.callbacks['on_predict_postprocess_end']), 1)

    def test_decisions_identical_enabled_disabled_mixed_sequence(self):
        outcomes = []
        for enabled in (False, True):
            self.writer.enabled = enabled
            self.motor = MotorV4(self.cfg)
            events, rows = [], []
            for frame in range(1, 130):
                ds = [detection()]
                if 16 <= frame <= 35:
                    ds = [detection(label='pessoa_normal')]
                elif frame in (2, 40):
                    ds = []
                elif frame == 3:
                    ds = [detection(None, .528)]
                elif frame == 4:
                    ds = [detection(score=.49)]
                events.extend(self.step(frame, ds))
                rows.append(copy.deepcopy(self.motor.rows))
            outcomes.append((events, rows))
        self.assertEqual(outcomes[0], outcomes[1])

    def test_sampling_gaps_aggregate_and_reset_effective_interruptions_only(self):
        observer = Observer(Config(log_mode='homologation', homologation_log_every=100), 'run')
        motor = MotorV4(Config(source_mode='stream'))
        motor.update_detections([detection()], 1, 0)
        motor.update_detections([], 2, 3)
        self.assertTrue(motor.continuity[0]['limpeza_efetiva'])
        motor.update_detections([], 3, 6)
        self.assertFalse(motor.continuity[0]['limpeza_efetiva'])
        with self.assertLogs('poolsafe.metrics', logging.INFO) as logs:
            for frame in range(1, 201):
                if frame in (5, 10, 20):
                    observer.sampling_gap(frame/10)
                if frame == 20:
                    observer.continuity_interrupted()
                observer.completed(frame, .01, .001, .02, .0)
        fields = [r.fields for r in logs.records]
        self.assertEqual([f['sampling_gap_count'] for f in fields], [3, 0])
        self.assertEqual([f['sampling_gap_max_s'] for f in fields], [2, 0])
        self.assertEqual([f['continuity_interruptions'] for f in fields], [1, 0])

    def test_stream_worker_aggregates_gaps_and_logs_real_cleanup_immediately(self):
        import main as worker
        from pipeline_fakes import FakeModelMulti
        model_path = Path(self.temp.name)/'best.pt'
        model_path.write_bytes(b'fixture')
        cfg = Config(log_mode='homologation', source_mode='stream', source='fixture',
                     model_path=str(model_path), data_dir=self.temp.name, captures_enabled=False)
        class Reader:
            def __init__(self, source, stopping, **kwargs):
                self.stopping, self.frame = stopping, 0
                self.started = time.monotonic()
            def start(self):
                pass
            def join(self, **kwargs):
                pass
            def take(self):
                self.frame += 1
                if self.frame > 100:
                    self.stopping.set()
                    return None
                return (self.frame, 1, object(),
                        self.started+self.frame*.6+(3 if self.frame >= 50 else 0),
                        '2026-11-02T10:00:00Z')
        model = FakeModelMulti()
        torch = SimpleNamespace(set_num_threads=lambda _: None, set_num_interop_threads=lambda _: None,
                                cuda=SimpleNamespace(is_available=lambda: False))
        with patch.object(worker, 'Config', return_value=cfg), patch.object(worker, 'StreamReader', Reader), \
             patch.dict(sys.modules, {'torch': torch, 'ultralytics': SimpleNamespace(YOLO=lambda _: model)}), \
             self.assertLogs(level=logging.INFO) as logs:
            worker.main()
        events = [getattr(r, 'event_name', '') for r in logs.records]
        self.assertNotIn('v4.sampling_gap', events)
        self.assertEqual(events.count('v4.continuity_interrupted'), 1)
        self.assertEqual(model.call_count, 100)
        summary = next(r.fields for r in logs.records if getattr(r, 'event_name', '') == 'worker.homologation_summary')
        self.assertEqual(summary['sampling_gap_count'], 99)
        self.assertAlmostEqual(summary['sampling_gap_max_s'], 3.6)
        self.assertEqual(summary['continuity_interruptions'], 1)


if __name__ == '__main__':
    unittest.main()
