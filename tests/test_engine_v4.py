import ast
import math
from collections import Counter, deque
import json
import random
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import Config
from engine_adapter import MotorV4, V4InputError
from motor_temporal_v4 import MotorTemporalContinuo

def detection(tid=1, label='pessoa_queda', score=0.9, bounds=(0, 0, 100, 100)):
    return dict(track_id=tid, classe=label, confianca=score, bbox_xyxy=list(bounds))
if __name__ == '__main__':
    unittest.main()

class EngineV4Test(unittest.TestCase):

    def cfg(self, **kwargs):
        c = Config()
        c.engine_version = 'v4'
        c.source_mode = 'video'
        for key, value in kwargs.items():
            setattr(c, key, value)
        return c

    def feed(self, motor, blocks, start=1, dt=0.1):
        events = []
        index = start
        for count, detections in blocks:
            for _ in range(count):
                events.extend((d for d in motor.update_detections(detections, index, (index - 1) * dt) if d.status == 'confirmado'))
                index += 1
        return events

    def test_default_policy(self):
        c = self.cfg()
        self.assertEqual(c.inference_conf, 0.3)
        self.assertEqual((c.frames_desequilibrio, c.frames_queda, c.fall_tolerance), (15, 10, 1))

    def test_ten_real_evidences_with_gap(self):
        m = MotorV4(self.cfg())
        events = self.feed(m, [(5, [detection()]), (1, []), (5, [detection()])])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event['frames_evidencias'], [1, 2, 3, 4, 5, 7, 8, 9, 10, 11])

    def test_two_gaps_rejected(self):
        self.assertFalse(self.feed(MotorV4(self.cfg()), [(5, [detection()]), (2, []), (5, [detection()])]))

    def test_class_thresholds(self):
        for label, score in [('pessoa_queda', 0.49), ('pessoa_desequilibrio', 0.64)]:
            self.assertFalse(self.feed(MotorV4(self.cfg()), [(30, [detection(label=label, score=score)])]))

    def test_attention_threshold_and_escalation(self):
        m = MotorV4(self.cfg())
        events = self.feed(m, [(15, [detection(label='pessoa_desequilibrio')]), (10, [detection()])])
        self.assertEqual([e.severity for e in events], ['ATENCAO', 'CRITICA'])
        self.assertEqual(events[1].event['tipo_evento'], 'elevacao_gravidade')
        self.assertEqual(events[0].event['evento_referencia'], events[1].event['evento_referencia'])

    def test_attention_can_be_disabled(self):
        events = self.feed(MotorV4(self.cfg(emit_attention=False)), [(15, [detection(label='pessoa_desequilibrio')]), (10, [detection()])])
        self.assertEqual([e.severity for e in events], ['CRITICA'])
        self.assertEqual(events[0].event['tipo_evento'], 'abertura_episodio')

    def test_cooldown_retries_persistent_candidate(self):
        m = MotorV4(self.cfg())
        events = self.feed(m, [(10, [detection()]), (40, [detection(2, bounds=(300, 0, 400, 100))])])
        self.assertEqual([e.track_id for e in events], [1, 2])
        self.assertGreaterEqual(events[1].event['tempo_video_s'], 3.9)

    def test_handoff_pending_only(self):
        events = self.feed(MotorV4(self.cfg()), [(5, [detection()]), (5, [detection(2)])])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].track_id, 2)

    def test_duplicate_fall_is_associated(self):
        events = self.feed(MotorV4(self.cfg(cooldown=0)), [(10, [detection()]), (10, [detection(2)])])
        self.assertEqual(len(events), 1)

    def test_recovery_requires_accepted_normality(self):
        m = MotorV4(self.cfg(cooldown=0))
        events = self.feed(m, [(10, [detection()]), (15, [detection(label='pessoa_normal', score=0.29)]), (10, [detection()])])
        self.assertEqual(len(events), 1)
        events = self.feed(m, [(15, [detection(label='pessoa_normal')]), (10, [detection()])], start=36)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event['episodio'], 2)

    def test_no_id_and_duplicate_frame(self):
        m = MotorV4(self.cfg())
        self.assertFalse(self.feed(m, [(20, [detection(tid=None)])]))
        with self.assertRaises(V4InputError):
            m.update_detections([], 20, 2)

    def test_time_validation(self):
        m = MotorV4(self.cfg())
        m.update_detections([], 1, 10)
        with self.assertRaises(V4InputError):
            m.update_detections([], 2, 9)
        with self.assertRaises(V4InputError):
            m.update_detections([], 2, float('nan'))

    def test_history_is_drained(self):
        m = MotorV4(self.cfg())
        self.feed(m, [(10, [detection()])])
        self.assertEqual((m.engine.events, m.engine.transitions, m.engine.auditoria_continuidade), ([], [], []))

    def test_random_video_parity(self):
        for seed in range(10):
            rng = random.Random(seed)
            cfg = self.cfg(cooldown=0)
            m = MotorV4(cfg)
            source = ast.parse((Path(__file__).resolve().parents[1]/'reference/v4_colab.py.txt').read_text())
            classes = [n for n in source.body if isinstance(n, ast.ClassDef) and n.name in ('MotorTemporal', 'MotorTemporalContinuo')]
            ns = dict(math=math, Counter=Counter, deque=deque, CLASSES={'pessoa_normal','pessoa_desequilibrio','pessoa_queda'}, CLASSES_RISCO=('pessoa_desequilibrio','pessoa_queda'))
            exec(compile(ast.Module(body=classes,type_ignores=[]), '<original>', 'exec'),ns)
            reference = ns['MotorTemporalContinuo']({'pessoa_normal': 0.3, 'pessoa_desequilibrio': 0.65, 'pessoa_queda': 0.5}, {'pessoa_desequilibrio': 15, 'pessoa_queda': 10}, {'pessoa_desequilibrio', 'pessoa_queda'}, cooldown=0, camera_id=cfg.camera_id, deduplicar_quedas=True)
            expected, actual = ([], [])
            label = 'pessoa_queda'
            for index in range(1, 501):
                if rng.random() < 0.1:
                    label = rng.choice(['pessoa_normal', 'pessoa_queda', 'pessoa_desequilibrio'])
                ds = [] if rng.random() < 0.04 else [detection(1, label, 0.9)]
                timestamp = (index - 1) / 15
                actual.extend((d.event for d in m.update_detections(ds, index, timestamp) if d.status == 'confirmado'))
                raw = [dict(track_id=d['track_id'], classe=d['classe'], confianca=d['confianca'], x1=0, y1=0, x2=100, y2=100) for d in ds]
                reference.step(index, timestamp, raw)
            expected = reference.events
            self.assertEqual(actual, expected)

    def test_worker_v4_publishes_existing_envelope(self):
        from pipeline_fakes import FakeModelMulti, FakeCapture, FakePublisher
        from types import SimpleNamespace
        import main as worker
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'best.pt').write_bytes(b'fixture')
            (root / 'video.mp4').write_bytes(b'fixture')
            cfg = self.cfg(model_path=str(root / 'best.pt'), source=str(root / 'video.mp4'), data_dir=str(root), publish=True, allow_test_publish=True, codigo_integracao='cam_test', rabbit_user='fixture', rabbit_password='fixture')
            cap = FakeCapture()
            cap.left = 25
            model = FakeModelMulti([0] * 15 + [2] * 10)
            fake_cv2 = SimpleNamespace(VideoCapture=lambda *_: cap, CAP_PROP_FPS=5, setNumThreads=lambda _: None)
            fake_torch = SimpleNamespace(set_num_threads=lambda _: None, set_num_interop_threads=lambda _: None, cuda=SimpleNamespace(is_available=lambda: False))
            with patch.object(worker, 'Config', return_value=cfg), patch.object(worker, 'Publisher', FakePublisher), patch.dict(sys.modules, {'torch': fake_torch, 'cv2': fake_cv2, 'ultralytics': SimpleNamespace(YOLO=lambda _: model)}):
                worker.main()
            run_id = (root / 'latest_run').read_text()
            summary = json.loads((root / 'runs' / run_id / 'resumo.json').read_text())
            self.assertEqual(summary['eventos']['confirmados_por_severidade'], {'ATENCAO': 1, 'CRITICA': 1})
            from transporte import Outbox
            outbox = Outbox(root / 'outbox.sqlite3')
            first = json.loads(outbox.first()[2])
            self.assertEqual(first['type'], 'POSSIVEL_RISCO')
            self.assertEqual(first['schemaVersion'], '1')
            self.assertEqual(summary['configuracao_efetiva']['politica_motor']['engine_version'], 'v4')
            self.assertNotIn('episodeKey', first['technicalData'])
            first_key = first['eventKey']
            outbox.remove(first_key)
            second = json.loads(outbox.first()[2])
            self.assertEqual(second['severity'], 'CRITICA')
            self.assertEqual(first['severity'], 'ATENCAO')
            self.assertNotEqual(second['eventKey'], first_key)
            from export_ensaio import generate_markdown, event_reference
            report = generate_markdown(summary)
            for event in summary['eventos']['lista_eventos']:
                self.assertIn('hash event_ref: ' + event['event_ref'], report)
            self.assertEqual(event_reference({'eventKey':'old'}),'eventKey original: old')

class RegressionTest(unittest.TestCase):
    cfg = EngineV4Test.cfg
    feed = EngineV4Test.feed
    def test_temporal_gap(self):
        for mode, n in [('stream',0),('video',1)]:
            m=MotorV4(self.cfg(source_mode=mode))
            self.feed(m,[(9,[detection()])])
            self.assertEqual(sum(e.status=='confirmado' for e in m.update_detections([detection()],10,120.)),n)
        m=MotorV4(self.cfg(source_mode='stream'))
        self.assertEqual(len(self.feed(m,[(10,[detection()])])),1)

    def test_partial_counts_and_active_episode(self):
        for label, count in [('pessoa_desequilibrio',14),('pessoa_normal',14)]:
            m=MotorV4(self.cfg(source_mode='stream'))
            self.feed(m,[(10,[detection()]),(count,[detection(label=label)])])
            m.update_detections([detection(label=label)],25,120.)
            self.assertTrue(m.get_track_state(1)['active_critica'])
            self.assertEqual(m.get_track_state(1)['normal' if label=='pessoa_normal' else 'streak_atencao'],1)
            self.assertFalse([e for e in m.update_detections([detection()],26,120.1) if e.status=='confirmado'])

    def test_attention_does_not_block_other_id_or_person(self):
        for tid, bounds in [(1,(0,0,100,100)),(2,(0,0,100,100)),(2,(300,0,400,100))]:
            m=MotorV4(self.cfg())
            events=self.feed(m,[(15,[detection(label='pessoa_desequilibrio')]),(40,[detection(tid,bounds=bounds)])])
            self.assertEqual([e.severity for e in events],['ATENCAO','CRITICA'])
            self.assertEqual(events[1].event['frame'],25)
            if tid==2:
                self.assertNotEqual(events[0].event['evento_referencia'],events[1].event['evento_referencia'])
