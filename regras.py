"""Confirmação temporal, independente de YOLO, rede ou banco.

O filtro não comprova uma queda: confirma persistência de classificações do modelo.
Um erro persistente também pode satisfazer a regra. ByteTrack pode trocar IDs.
"""
from dataclasses import dataclass


@dataclass
class Estado:
    streak: int = 0
    normal: int = 0
    active: bool = False
    last_seen: float = -1.0
    last_frame: int = -1


class Motor:
    def __init__(self, frames=8, normal_frames=15, cooldown=3, max_gap=.5, lost_reset=2):
        self.frames, self.normal_frames = frames, normal_frames
        self.cooldown, self.max_gap, self.lost_reset = cooldown, max_gap, lost_reset
        self.states = {}
        self.last_alert = float('-inf')

    def update(self, observations, frame_number, now):
        """observations: {track_id: bool_em_queda}.

        now usa relógio monotônico no stream e tempo nominal no arquivo.
        Ausência interrompe a sequência; o episódio fica retido brevemente para
        evitar rearmar a cada lacuna. Cooldown é POR CÂMERA e pode suprimir outro
        indivíduo: limitação explícita para avaliação, não deduplicação perfeita.
        """
        decisions = []
        for tid, s in list(self.states.items()):
            if now - s.last_seen > self.lost_reset:
                del self.states[tid]
            elif tid not in observations:
                s.streak = s.normal = 0
        for tid, risk in observations.items():
            s = self.states.setdefault(tid, Estado())
            consecutive = (s.last_frame == frame_number - 1 and
                           0 <= now - s.last_seen <= self.max_gap)
            if not consecutive:
                s.streak = s.normal = 0
            s.last_seen, s.last_frame = now, frame_number
            if risk:
                s.normal = 0
                s.streak += 1
                if s.streak >= self.frames and not s.active:
                    s.active = True
                    # Mesmo episódio suprimido fica ativo: não é enviado mais tarde
                    # como se tivesse ocorrido um evento novo.
                    allowed = now - self.last_alert >= self.cooldown
                    if allowed:
                        self.last_alert = now
                    decisions.append((tid, 'confirmado' if allowed else 'suprimido_cooldown'))
            else:
                s.streak = 0
                s.normal += 1
                if s.normal >= self.normal_frames:
                    s.active = False
        return decisions
