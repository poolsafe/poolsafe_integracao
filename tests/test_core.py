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
from regras import Motor, validate_model_classes, ModelCompatibilityError, resolve_observation, Decisao
from transporte import Outbox, Publisher
import export_ensaio


class RegrasTest(unittest.TestCase):
    def motor(self, **kw):
        return Motor(frames=3, normal_frames=2, cooldown=3, **kw)

    def test_requer_sequencia_e_nao_repete_episodio(self):
        m = self.motor()
        self.assertEqual(m.update({1: True}, 1, 0), [])
        self.assertEqual(m.update({1: True}, 2, .1), [])
        self.assertEqual(m.update({1: True}, 3, .2), [(1, 'confirmado', 'CRITICA')])
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


class ValidacaoModeloTest(unittest.TestCase):
    def test_modelo_tres_classes_valido(self):
        names = {0: 'pessoa_desequilibrio', 1: 'pessoa_normal', 2: 'pessoa_queda'}
        res = validate_model_classes(names)
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0], 'pessoa_desequilibrio')
        self.assertEqual(res[1], 'pessoa_normal')
        self.assertEqual(res[2], 'pessoa_queda')

    def test_modelo_duas_classes_retrocompativel(self):
        names = {0: 'pessoa_normal', 1: 'pessoa_queda'}
        res = validate_model_classes(names)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0], 'pessoa_normal')
        self.assertEqual(res[1], 'pessoa_queda')

    def test_modelo_classe_obrigatoria_ausente(self):
        # Falta pessoa_queda
        with self.assertRaises(ModelCompatibilityError) as ctx:
            validate_model_classes({0: 'pessoa_normal', 1: 'pessoa_desequilibrio'})
        self.assertEqual(ctx.exception.code, 'ERR_MODEL_CLASSES_MISSING')
        self.assertIn('pessoa_queda', ctx.exception.message)

        # Falta pessoa_normal
        with self.assertRaises(ModelCompatibilityError) as ctx:
            validate_model_classes({0: 'pessoa_queda'})
        self.assertEqual(ctx.exception.code, 'ERR_MODEL_CLASSES_MISSING')

    def test_modelo_classe_desconhecida_rejeitada(self):
        names = {0: 'pessoa_normal', 1: 'pessoa_queda', 2: 'animal'}
        with self.assertRaises(ModelCompatibilityError) as ctx:
            validate_model_classes(names)
        self.assertEqual(ctx.exception.code, 'ERR_MODEL_CLASSES_UNKNOWN')
        self.assertIn('animal', ctx.exception.message)

    def test_observacao_classe_desconhecida_nao_interpretada_como_normal(self):
        with self.assertRaises(ModelCompatibilityError) as ctx:
            resolve_observation('intruso_desconhecido')
        self.assertEqual(ctx.exception.code, 'ERR_UNKNOWN_CLASS')


class RegrasMultiClasseTest(unittest.TestCase):
    def motor(self, **kw):
        params = {'frames': 3, 'normal_frames': 2, 'cooldown': 3.0}
        params.update(kw)
        return Motor(**params)

    def test_normal_sem_evento(self):
        m = self.motor()
        for i in range(15):
            decisions = m.update({1: 'pessoa_normal'}, i + 1, i * 0.1)
            self.assertEqual(decisions, [])
        self.assertEqual(len(m.states), 1)
        self.assertFalse(m.states[1].active)

    def test_desequilibrio_gerando_somente_atencao(self):
        m = self.motor()
        self.assertEqual(m.update({1: 'pessoa_desequilibrio'}, 1, 0.0), [])
        self.assertEqual(m.update({1: 'pessoa_desequilibrio'}, 2, 0.1), [])
        d = m.update({1: 'pessoa_desequilibrio'}, 3, 0.2)
        self.assertEqual(d, [(1, 'confirmado', 'ATENCAO')])
        # Frames subsequentes de desequilíbrio não repetem o evento
        self.assertEqual(m.update({1: 'pessoa_desequilibrio'}, 4, 0.3), [])

    def test_queda_gerando_critica(self):
        m = self.motor()
        self.assertEqual(m.update({1: 'pessoa_queda'}, 1, 0.0), [])
        self.assertEqual(m.update({1: 'pessoa_queda'}, 2, 0.1), [])
        d = m.update({1: 'pessoa_queda'}, 3, 0.2)
        self.assertEqual(d, [(1, 'confirmado', 'CRITICA')])
        # Frames subsequentes de queda não repetem
        self.assertEqual(m.update({1: 'pessoa_queda'}, 4, 0.3), [])

    def test_frames_classes_diferentes_nao_somam_confirmacao(self):
        m = self.motor(frames=4)
        # 2 frames de desequilíbrio + 2 frames de queda != 4 frames de queda
        m.update({1: 'pessoa_desequilibrio'}, 1, 0.0)
        m.update({1: 'pessoa_desequilibrio'}, 2, 0.1)
        self.assertEqual(m.states[1].streak_atencao, 2)
        self.assertEqual(m.states[1].streak_critica, 0)

        # Transição para queda reseta a contagem de atenção e inicia a crítica do zero
        m.update({1: 'pessoa_queda'}, 3, 0.2)
        self.assertEqual(m.states[1].streak_atencao, 0)
        self.assertEqual(m.states[1].streak_critica, 1)

        d = m.update({1: 'pessoa_queda'}, 4, 0.3)
        self.assertEqual(d, [])  # Apenas 2 frames de queda acumulados, nenhum alerta
        self.assertEqual(m.states[1].streak_critica, 2)

    def test_atencao_seguida_de_queda_sem_bloqueio_por_cooldown_ou_flag(self):
        m = self.motor(frames=3, cooldown=3.0)
        # 3 frames de desequilíbrio -> confirma ATENCAO aos 0.2s
        m.update({1: 'pessoa_desequilibrio'}, 1, 0.0)
        m.update({1: 'pessoa_desequilibrio'}, 2, 0.1)
        d_atencao = m.update({1: 'pessoa_desequilibrio'}, 3, 0.2)
        self.assertEqual(d_atencao, [(1, 'confirmado', 'ATENCAO')])
        self.assertTrue(m.states[1].active_atencao)
        self.assertFalse(m.states[1].active_critica)

        # Queda inicia imediatamente a seguir (aos 0.3s, dentro da janela de cooldown global de 3s)
        m.update({1: 'pessoa_queda'}, 4, 0.3)
        m.update({1: 'pessoa_queda'}, 5, 0.4)
        d_critica = m.update({1: 'pessoa_queda'}, 6, 0.5)

        # CRITICA NÃO pode ser bloqueada pelo cooldown ou pela flag de ATENCAO ativa
        self.assertEqual(d_critica, [(1, 'confirmado', 'CRITICA')])
        self.assertTrue(m.states[1].active_critica)

    def test_critica_sem_regressao_para_nova_atencao_no_mesmo_episodio(self):
        m = self.motor(frames=3)
        # 3 frames de queda -> confirma CRITICA
        for i in range(3):
            m.update({1: 'pessoa_queda'}, i + 1, i * 0.1)
        self.assertTrue(m.states[1].active_critica)

        # Oscilação para desequilíbrio não deve emitir ATENCAO após CRITICA antes de rearmar
        for i in range(5):
            d = m.update({1: 'pessoa_desequilibrio'}, i + 4, (i + 4) * 0.1)
            self.assertEqual(d, [])

    def test_rearmamento_por_frames_normais(self):
        m = self.motor(frames=3, normal_frames=2)
        # Emite crítica
        for i in range(3):
            m.update({1: 'pessoa_queda'}, i + 1, i * 0.1)
        self.assertTrue(m.states[1].active_critica)

        # 2 frames normais rearmam o track
        m.update({1: 'pessoa_normal'}, 4, 0.3)
        self.assertTrue(m.states[1].active_critica)
        m.update({1: 'pessoa_normal'}, 5, 0.4)
        self.assertFalse(m.states[1].active_critica)
        self.assertFalse(m.states[1].active_atencao)

        # Após rearmar e esperar término do cooldown da severidade (3s), novo evento pode ser emitido
        for i in range(2):
            m.update({1: 'pessoa_desequilibrio'}, i + 6, 4.0 + i * 0.1)
        d_novo = m.update({1: 'pessoa_desequilibrio'}, 8, 4.2)
        self.assertEqual(d_novo, [(1, 'confirmado', 'ATENCAO')])

    def test_interrupcao_por_lacuna_tempo_e_ausencia(self):
        m = self.motor(frames=3, max_gap=0.5)
        # 2 frames de desequilíbrio
        m.update({1: 'pessoa_desequilibrio'}, 1, 0.0)
        m.update({1: 'pessoa_desequilibrio'}, 2, 0.1)

        # Lacuna de tempo (> max_gap de 0.5s) quebra a sequência
        m.update({1: 'pessoa_desequilibrio'}, 3, 1.0)
        self.assertEqual(m.states[1].streak_atencao, 1)

        # 2 frames de queda
        m.update({1: 'pessoa_queda'}, 4, 1.1)
        m.update({1: 'pessoa_queda'}, 5, 1.2)
        self.assertEqual(m.states[1].streak_critica, 2)

        # Ausência do ID no frame 6 quebra a sequência de queda
        m.update({}, 6, 1.3)
        self.assertEqual(m.states[1].streak_critica, 0)
        m.update({1: 'pessoa_queda'}, 7, 1.4)
        self.assertEqual(m.states[1].streak_critica, 1)

    def test_perda_de_id_apos_lost_reset(self):
        m = self.motor(lost_reset=2.0)
        m.update({1: 'pessoa_normal'}, 1, 0.0)
        self.assertIn(1, m.states)

        # Mais de 2.0s sem observação purga o ID
        m.update({}, 2, 3.0)
        self.assertNotIn(1, m.states)

    def test_cooldown_separado_por_severidade_outro_id(self):
        m = self.motor(frames=3, cooldown=3.0)
        # Track 1 confirma ATENCAO aos 0.2s
        for i in range(3):
            m.update({1: 'pessoa_desequilibrio'}, i + 1, i * 0.1)

        # Track 2 atinge 3 frames de ATENCAO aos 1.2s -> suprimido pelo cooldown de ATENCAO
        m.update({2: 'pessoa_desequilibrio'}, 10, 1.0)
        m.update({2: 'pessoa_desequilibrio'}, 11, 1.1)
        d2_atencao = m.update({2: 'pessoa_desequilibrio'}, 12, 1.2)
        self.assertEqual(d2_atencao, [(2, 'suprimido_cooldown', 'ATENCAO')])

        # Track 2 transiciona para queda e completa 3 frames de CRITICA aos 1.5s
        m.update({2: 'pessoa_queda'}, 13, 1.3)
        m.update({2: 'pessoa_queda'}, 14, 1.4)
        d2_critica = m.update({2: 'pessoa_queda'}, 15, 1.5)
        # CRITICA confirma porque o cooldown de CRITICA é independente e nunca foi acionado
        self.assertEqual(d2_critica, [(2, 'confirmado', 'CRITICA')])


class ExportEnsaioTest(unittest.TestCase):
    def test_geracao_relatorio_markdown_e_redacao_credenciais(self):
        sample_data = {
            'run_id': 'run-teste-123',
            'inicio': '2026-10-25T20:00:00.000Z',
            'fim': '2026-10-25T20:00:10.000Z',
            'motivo_encerramento': 'fim_esperado_video',
            'detalhe_encerramento': 'Todos os 300 frames lidos',
            'log_mode': 'development',
            'source_mode': 'stream',
            'source': 'rtsp://admin:super_secret_pass@192.168.1.100:554/live',
            'revisao_codigo': 'abc123commit',
            'configuracao_efetiva': {
                'camera_id': 'cam_01',
                'device': 'cpu',
                'frames_para_alerta': 8,
                'cooldown_s': 3.0,
                'test_mode': True,
                'publish_rabbit': False
            },
            'modelo': {
                'versao': 'modelo-003',
                'sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
                'classes': {0: 'pessoa_desequilibrio', 1: 'pessoa_normal', 2: 'pessoa_queda'}
            },
            'processamento': {
                'frames_processados': 300,
                'duracao_total_s': 10.0,
                'fps_efetivo': 30.0,
                'tempo_primeiro_frame_s': 0.12,
                'tempo_medio_frames_subsequentes_s': 0.033,
                'tempo_min_frame_s': 0.025,
                'tempo_medio_frame_s': 0.033,
                'tempo_max_frame_s': 0.12
            },
            'eventos': {
                'confirmados_por_severidade': {'ATENCAO': 1, 'CRITICA': 1},
                'suprimidos_por_severidade': {'ATENCAO': 0, 'CRITICA': 0},
                'lista_eventos': [
                    {
                        'eventKey': 'uuid-atencao',
                        'severity': 'ATENCAO',
                        'classe': 'pessoa_desequilibrio',
                        'track_id': 5,
                        'frame_processado': 50,
                        'video_time_s': 1.66,
                        'detectedAt': '2026-10-25T20:00:02.000Z',
                        'confidence': 0.85
                    },
                    {
                        'eventKey': 'uuid-critica',
                        'severity': 'CRITICA',
                        'classe': 'pessoa_queda',
                        'track_id': 5,
                        'frame_processado': 60,
                        'video_time_s': 2.0,
                        'detectedAt': '2026-10-25T20:00:03.000Z',
                        'confidence': 0.92
                    }
                ]
            },
            'outbox': {
                'pendencias_finais': 0
            }
        }
        md = export_ensaio.generate_markdown(sample_data)
        # Credencial de câmera DEVE ser mascarada
        self.assertNotIn('super_secret_pass', md)
        self.assertIn('rtsp://admin:***@192.168.1.100:554/live', md)
        # Eventos por severidade presentes
        self.assertIn('ATENCAO', md)
        self.assertIn('CRITICA', md)
        self.assertIn('uuid-atencao', md)
        self.assertIn('uuid-critica', md)
        # Metadados essenciais
        self.assertIn('run-teste-123', md)
        self.assertIn('fim_esperado_video', md)
        self.assertIn('30.0 FPS', md)


if __name__ == '__main__':
    unittest.main()

