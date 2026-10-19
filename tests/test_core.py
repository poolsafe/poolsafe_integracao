"""Testes de comportamentos críticos sem modelo, câmera ou credenciais reais."""
import json
from pathlib import Path
import sys
import tempfile
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


if __name__ == '__main__':
    unittest.main()
