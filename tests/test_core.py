"""Testes de comportamentos críticos sem modelo, câmera ou credenciais reais."""
import json
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from regras import Motor
from transporte import Outbox, Publisher


class RegrasTest(unittest.TestCase):
    def motor(self, **kw):
        return Motor(frames=3, normal_frames=2, cooldown=3, **kw)

    def test_requer_sequencia_e_nao_repete_episodio(self):
        m = self.motor()
        self.assertEqual(m.update({1: True}, 1, 0), [])
        self.assertEqual(m.update({1: True}, 2, .1), [])
        self.assertEqual(m.update({1: True}, 3, .2), [(1, 'confirmado')])
        self.assertEqual(m.update({1: True}, 4, .3), [])

    def test_ausencia_quebra_contagem(self):
        m = self.motor()
        m.update({1: True}, 1, 0)
        m.update({1: True}, 2, .1)
        m.update({}, 3, .2)
        self.assertEqual(m.update({1: True}, 4, .3), [])

    def test_lacuna_de_tempo_quebra_sequencia(self):
        m = self.motor()
        m.update({1: True}, 1, 0)
        m.update({1: True}, 2, .1)
        self.assertEqual(m.update({1: True}, 3, 1), [])

    def test_ids_nao_somam_frames(self):
        m = self.motor()
        m.update({1: True}, 1, 0)
        m.update({1: True}, 2, .1)
        self.assertEqual(m.update({2: True}, 3, .2), [])

    def test_cooldown_suprime_outro_id_sem_alerta_tardio(self):
        m = self.motor()
        for i in range(3):
            m.update({1: True, 2: True}, i + 1, i / 10)
        self.assertTrue(m.states[2].active)
        self.assertEqual(m.last_alert, .2)
        # O mesmo episódio não se transforma em alerta novo ao terminar cooldown.
        self.assertEqual(m.update({2: True}, 4, 3.5), [])

    def test_normais_rearmam(self):
        m = self.motor()
        for i in range(3):
            m.update({1: True}, i+1, i/10)
        m.update({1: False}, 4, .3)
        self.assertTrue(m.states[1].active)
        m.update({1: False}, 5, .4)
        self.assertFalse(m.states[1].active)


class OutboxTest(unittest.TestCase):
    def test_reabertura_preserva_identidade_e_payload(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'outbox.db'
            a = Outbox(path)
            a.put({'event_id': 'abc', 'tipo': 'alerta'}, 1000)
            b = Outbox(path)
            self.assertEqual(b.first()[0], 'abc')
            self.assertEqual(json.loads(b.first()[2])['tipo'], 'alerta')
            b.remove('abc')
            self.assertEqual(a.count(), 0)

    def test_capacidade_nao_apaga_pendente(self):
        with tempfile.TemporaryDirectory() as d:
            b = Outbox(Path(d)/'o.db', 1)
            b.put({'event_id': 'primeiro'}, 1000)
            with self.assertRaises(RuntimeError):
                b.put({'event_id': 'segundo'}, 1000)
            self.assertEqual(b.first()[0], 'primeiro')

    def test_heartbeat_substitui_o_anterior(self):
        p = Publisher(None, None)
        p.heartbeat({'event_id': 'velho'}, 1)
        p.heartbeat({'event_id': 'novo'}, 2)
        self.assertEqual(p.pending_hb, ({'event_id': 'novo'}, 2))

    def test_publicacao_mandatory_persistencia_e_expiracao(self):
        cfg = SimpleNamespace(exchange='teste', alert_key='alerta', heartbeat_key='heartbeat')
        p = Publisher(cfg, None)
        p.channel = Mock()
        # Stub mínimo permite testar o contrato sem instalar Pika no ambiente de QA.
        pika = SimpleNamespace(BasicProperties=lambda **kw: kw)
        with patch.dict(sys.modules, {'pika': pika}), patch('transporte.time.time', return_value=100):
            self.assertFalse(p.send({'event_id': 'x'}, 99, True))
            p.channel.basic_publish.assert_not_called()
            p.send({'event_id': 'x', 'tipo': 'alerta'}, 130, True)
        sent = p.channel.basic_publish.call_args.kwargs
        self.assertTrue(sent['mandatory'])
        self.assertEqual(sent['properties']['delivery_mode'], 2)
        self.assertEqual(sent['properties']['expiration'], '30000')
        self.assertEqual(sent['properties']['message_id'], 'x')

    def test_falha_broker_preserva_outbox(self):
        with tempfile.TemporaryDirectory() as d:
            b = Outbox(Path(d)/'o.db')
            b.put({'event_id': 'preservar'}, 10**12)
            p = Publisher(None, b)
            p.conn = SimpleNamespace(is_closed=False, is_open=False)
            def fail(*a, **k):
                p.stop_event.set()
                raise ConnectionError('sem broker')
            p.send = fail
            p.run()
            self.assertEqual(b.first()[0], 'preservar')

    def test_ausencia_declaracao_topologia(self):
        cfg = SimpleNamespace(
            rabbit_host='rabbitmq', rabbit_port=5672, rabbit_vhost='poolsafe',
            rabbit_user='u', rabbit_password='p', rabbit_tls=False,
            exchange='poolsafe.events'
        )
        p = Publisher(cfg, None)
        mock_channel = Mock()
        mock_conn = Mock()
        mock_conn.channel.return_value = mock_channel
        mock_pika = SimpleNamespace(
            BlockingConnection=Mock(return_value=mock_conn),
            PlainCredentials=Mock(),
            ConnectionParameters=Mock()
        )
        with patch.dict(sys.modules, {'pika': mock_pika}):
            p.connect()
        mock_channel.confirm_delivery.assert_called_once()
        self.assertFalse(hasattr(mock_channel, 'exchange_declare') and mock_channel.exchange_declare.called)
        self.assertFalse(hasattr(mock_channel, 'queue_declare') and mock_channel.queue_declare.called)
        self.assertFalse(hasattr(mock_channel, 'queue_bind') and mock_channel.queue_bind.called)

    def test_risco_sem_ttl_por_padrao(self):
        cfg = SimpleNamespace(exchange='poolsafe.events', alert_key='risk.detected', heartbeat_key='camera.heartbeat')
        p = Publisher(cfg, None)
        p.channel = Mock()
        pika = SimpleNamespace(BasicProperties=lambda **kw: kw)
        event = {
            'schemaVersion': '1',
            'eventKey': 'uuid-123',
            'cameraCode': 'cam-01',
            'type': 'POSSIVEL_RISCO',
            'severity': 'CRITICA',
            'detectedAt': '2026-10-21T01:00:00.000Z'
        }
        with patch.dict(sys.modules, {'pika': pika}):
            p.send(event, 0, risk=True)
        sent = p.channel.basic_publish.call_args.kwargs
        self.assertNotIn('expiration', sent['properties'])
        self.assertEqual(sent['properties']['delivery_mode'], 2)
        self.assertEqual(sent['properties']['message_id'], 'uuid-123')
        self.assertEqual(sent['properties']['type'], 'POSSIVEL_RISCO')

    def test_excecao_evidencia_sem_perder_evento(self):
        from main import prepare_evidence
        fake_empty_frame = SimpleNamespace(shape=(0, 0, 3))
        self.assertIsNone(prepare_evidence(fake_empty_frame))

        fake_frame = SimpleNamespace(shape=(480, 640, 3))
        with patch.dict(sys.modules, {'cv2': SimpleNamespace(resize=Mock(side_effect=RuntimeError('cv2 boom')))}):
            self.assertIsNone(prepare_evidence(fake_frame))

        mock_cv2 = SimpleNamespace(
            resize=Mock(return_value=object()),
            imencode=Mock(return_value=(False, None)),
            IMWRITE_JPEG_QUALITY=70
        )
        with patch.dict(sys.modules, {'cv2': mock_cv2}):
            self.assertIsNone(prepare_evidence(fake_frame))

        mock_cv2_large = SimpleNamespace(
            resize=Mock(return_value=object()),
            imencode=Mock(return_value=(True, b'x' * 300_000)),
            IMWRITE_JPEG_QUALITY=70
        )
        with patch.dict(sys.modules, {'cv2': mock_cv2_large}):
            self.assertIsNone(prepare_evidence(fake_frame))

        mock_cv2_ok = SimpleNamespace(
            resize=Mock(return_value=object()),
            imencode=Mock(return_value=(True, b'jpeg-bytes')),
            IMWRITE_JPEG_QUALITY=70
        )
        with patch.dict(sys.modules, {'cv2': mock_cv2_ok}):
            ev = prepare_evidence(fake_frame)
            self.assertIsNotNone(ev)
            self.assertEqual(ev['mimeType'], 'image/jpeg')
            self.assertEqual(ev['base64'], 'anBlZy1ieXRlcw==')

    def test_bloqueio_publicacao_teste_sem_liberacao_explicita(self):
        from config import Config
        with tempfile.TemporaryDirectory() as d:
            model_file = Path(d) / 'best.pt'
            model_file.write_bytes(b'fake')
            video_file = Path(d) / 'test.mp4'
            video_file.write_bytes(b'fake')

            c = Config()
            c.model_path = str(model_file)
            c.source = str(video_file)
            c.source_mode = 'video'
            c.test = True
            c.publish = True
            c.rabbit_user = 'user'
            c.rabbit_password = 'pwd'
            c.codigo_integracao = 'cam_01'
            c.allow_test_publish = False

            with self.assertRaises(ValueError) as ctx:
                c.validate()
            self.assertIn('ALLOW_TEST_PUBLISH', str(ctx.exception))

            c.allow_test_publish = True
            c.codigo_integracao = ''
            with self.assertRaises(ValueError) as ctx:
                c.validate()
            self.assertIn('CODIGO_INTEGRACAO', str(ctx.exception))

            c.codigo_integracao = 'cam_01'
            c.validate()

    def test_compatibilidade_pendencias_legadas(self):
        from transporte import adapt_legacy_payload
        legacy_alert = {
            'schema_version': 1,
            'event_id': 'uuid-legado-456',
            'tipo': 'alerta',
            'codigo_integracao': 'codigo-cadastrado',
            'observado_em': '2026-10-19T01:00:00.000Z',
            'confirmado_em': '2026-10-19T01:00:00.100Z',
            'teste': True,
            'origem': 'video',
            'run_id': 'run-1',
            'segmento': 0,
            'frame_processado': 10,
            'video_time_s': 2.5,
            'modelo': {'versao': 'candidato', 'sha256': 'hash64'},
            'deteccao': {'track_id': 5, 'confianca': 0.88},
            'evidencia': {'mime_type': 'image/jpeg', 'jpeg_base64': 'YWJj'}
        }
        adapted = adapt_legacy_payload(legacy_alert)
        self.assertEqual(adapted['schemaVersion'], '1')
        self.assertEqual(adapted['eventKey'], 'uuid-legado-456')
        self.assertEqual(adapted['cameraCode'], 'codigo-cadastrado')
        self.assertEqual(adapted['type'], 'POSSIVEL_RISCO')
        self.assertEqual(adapted['severity'], 'CRITICA')
        self.assertEqual(adapted['detectedAt'], '2026-10-19T01:00:00.000Z')
        self.assertEqual(adapted['confidence'], 0.88)
        self.assertEqual(adapted['technicalData']['trackId'], 5)
        self.assertEqual(adapted['technicalData']['videoTimeSeconds'], 2.5)
        self.assertEqual(adapted['evidence']['mimeType'], 'image/jpeg')
        self.assertEqual(adapted['evidence']['base64'], 'YWJj')

        legacy_hb = {
            'tipo': 'heartbeat',
            'event_id': 'hb-123',
            'codigo_integracao': 'cam-code',
            'observado_em': '2026-10-19T01:00:00.000Z',
            'expira_em': '2026-10-19T01:01:00.000Z',
            'teste': True
        }
        adapted_hb = adapt_legacy_payload(legacy_hb)
        self.assertEqual(adapted_hb['schemaVersion'], '1')
        self.assertEqual(adapted_hb['eventKey'], 'hb-123')
        self.assertEqual(adapted_hb['type'], 'CAMERA_HEARTBEAT')
        self.assertEqual(adapted_hb['cameraCode'], 'cam-code')

    def test_heartbeat_independente_do_avanco_inferencia(self):
        from captura import StreamReader
        captured_events = []
        stopping = threading.Event()
        def callback(observed_iso, observed_epoch):
            captured_events.append((observed_iso, observed_epoch))

        class MockCap:
            def __init__(self):
                self.count = 2
            def isOpened(self):
                return True
            def read(self):
                if self.count > 0:
                    self.count -= 1
                    return True, object()
                stopping.set()
                return False, None
            def release(self):
                pass

        reader = StreamReader('mock_source', stopping, on_frame_decoded=callback)
        with patch.dict(sys.modules, {'cv2': SimpleNamespace(VideoCapture=lambda *_, **__: MockCap(), CAP_FFMPEG=1, CAP_PROP_OPEN_TIMEOUT_MSEC=1, CAP_PROP_READ_TIMEOUT_MSEC=1)}):
            reader.run()

        self.assertEqual(len(captured_events), 2)
        self.assertTrue(captured_events[0][0].endswith('Z'))

    def test_erro_irrecuperavel_encerra_preservando_outbox(self):
        with tempfile.TemporaryDirectory() as d:
            b = Outbox(Path(d) / 'o.db')
            b.put({'event_id': 'evt_preservado'}, 10**12)
            stopping = threading.Event()
            p = Publisher(None, b, stopping=stopping)
            p.conn = SimpleNamespace(is_closed=False, is_open=False)

            class ProbableAuthenticationError(Exception):
                pass

            def auth_error(*a, **k):
                raise ProbableAuthenticationError('credenciais invalidas')

            p.send = auth_error
            p.run()

            self.assertIsNotNone(p.fatal_error)
            self.assertIn('ProbableAuthenticationError', p.fatal_error)
            self.assertTrue(stopping.is_set())
            self.assertEqual(b.count(), 1)
            self.assertEqual(b.first()[0], 'evt_preservado')

    def test_falha_transitoria_mantem_reconexao_sem_fatal_error(self):
        with tempfile.TemporaryDirectory() as d:
            b = Outbox(Path(d) / 'o.db')
            b.put({'event_id': 'evt_transitorio'}, 10**12)
            stopping = threading.Event()
            p = Publisher(None, b, stopping=stopping)
            def fake_connect():
                p.conn = SimpleNamespace(is_closed=False, is_open=False, process_data_events=Mock())
                p.channel = Mock()
            p.connect = Mock(side_effect=fake_connect)
            fake_connect()
            p.stop_event.wait = Mock()

            attempts = 0
            def transient_failure_then_success(*a, **k):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise ConnectionError('broker temporariamente fora')
                p.stop_event.set()
                return True

            p.send = transient_failure_then_success
            p.run()

            self.assertIsNone(p.fatal_error)
            self.assertFalse(stopping.is_set())
            self.assertEqual(b.count(), 0)


if __name__ == '__main__':
    unittest.main()
