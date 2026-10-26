"""Confirmação temporal, independente de YOLO, rede ou banco.

O filtro não comprova uma queda: confirma persistência de classificações do modelo.
Um erro persistente também pode satisfazer a regra. ByteTrack pode trocar IDs.
"""
from dataclasses import dataclass
from typing import NamedTuple, Optional, Dict, Any, List

# Mapeamento centralizado de classes e severidades
RECOGNIZED_CLASSES = {'pessoa_normal', 'pessoa_desequilibrio', 'pessoa_queda'}
REQUIRED_CLASSES = {'pessoa_normal', 'pessoa_queda'}

SEVERITY_BY_CLASS = {
    'pessoa_desequilibrio': 'ATENCAO',
    'pessoa_queda': 'CRITICA',
}


class ModelCompatibilityError(ValueError):
    """Erro seguro e identificável de compatibilidade do modelo de visão."""
    def __init__(self, code: str, message: str, found: Optional[List[str]] = None, expected: Optional[List[str]] = None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.found = sorted(found) if found else []
        self.expected = sorted(expected) if expected else []


def validate_model_classes(names_dict: Dict[int, str]) -> Dict[int, str]:
    """Valida classes do modelo por nome, sem fixar IDs numéricos.

    Aceita o modelo novo (3 classes) e preserva compatibilidade com o modelo anterior (2 classes).
    Não aceita classes desconhecidas e não as interpreta silenciosamente como normais.
    """
    normalized = {int(k): str(v).strip().lower() for k, v in names_dict.items()}
    found_classes = set(normalized.values())

    missing = REQUIRED_CLASSES - found_classes
    if missing:
        raise ModelCompatibilityError(
            code='ERR_MODEL_CLASSES_MISSING',
            message=(
                f"Classes obrigatórias ausentes nos pesos do modelo: {sorted(missing)}. "
                f"Encontradas: {sorted(found_classes)}, necessárias ao funcionamento: {sorted(REQUIRED_CLASSES)}."
            ),
            found=list(found_classes),
            expected=list(REQUIRED_CLASSES)
        )

    unknown = found_classes - RECOGNIZED_CLASSES
    if unknown:
        raise ModelCompatibilityError(
            code='ERR_MODEL_CLASSES_UNKNOWN',
            message=(
                f"Classes desconhecidas nos pesos do modelo: {sorted(unknown)}. "
                f"Classes reconhecidas: {sorted(RECOGNIZED_CLASSES)}."
            ),
            found=list(found_classes),
            expected=list(RECOGNIZED_CLASSES)
        )

    return normalized


def resolve_observation(obs: Any) -> str:
    """Normaliza a observação para 'NORMAL', 'ATENCAO' ou 'CRITICA'.

    Rejeita classes desconhecidas com erro identificável (nunca interpreta silenciosamente como normal).
    Preserva compatibilidade com booleano (True -> CRITICA, False -> NORMAL).
    """
    if isinstance(obs, bool):
        return 'CRITICA' if obs else 'NORMAL'
    if isinstance(obs, str):
        clean = obs.strip().lower()
        if clean in ('pessoa_queda', 'critica'):
            return 'CRITICA'
        if clean in ('pessoa_desequilibrio', 'atencao', 'atenção'):
            return 'ATENCAO'
        if clean in ('pessoa_normal', 'normal'):
            return 'NORMAL'
        raise ModelCompatibilityError(
            code='ERR_UNKNOWN_CLASS',
            message=f"Classe desconhecida observada na inferência: '{obs}'. Classes aceitas: {sorted(RECOGNIZED_CLASSES)}",
            found=[obs],
            expected=list(RECOGNIZED_CLASSES)
        )
    raise ValueError(f"Tipo de observação inválido: {type(obs).__name__}")


class Decisao(NamedTuple):
    track_id: int
    status: str       # 'confirmado' | 'suprimido_cooldown'
    severity: str     # 'ATENCAO' | 'CRITICA'


@dataclass
class Estado:
    streak_atencao: int = 0
    streak_critica: int = 0
    normal: int = 0
    active_atencao: bool = False
    active_critica: bool = False
    last_seen: float = -1.0
    last_frame: int = -1
    candidate_start_atencao_time: Optional[float] = None
    candidate_start_atencao_frame: Optional[int] = None
    candidate_start_critica_time: Optional[float] = None
    candidate_start_critica_frame: Optional[int] = None

    @property
    def active(self) -> bool:
        """Compatibilidade com testes e código anteriores."""
        return self.active_critica or self.active_atencao

    @active.setter
    def active(self, value: bool):
        self.active_critica = value
        self.active_atencao = value

    @property
    def streak(self) -> int:
        """Compatibilidade com testes anteriores."""
        return self.streak_critica or self.streak_atencao

    @streak.setter
    def streak(self, value: int):
        self.streak_critica = value
        self.streak_atencao = value


class Motor:
    def __init__(self, frames: int = 8, normal_frames: int = 15, cooldown: float = 3.0,
                 max_gap: float = 0.5, lost_reset: float = 2.0):
        self.frames = frames
        self.normal_frames = normal_frames
        self.cooldown = cooldown
        self.max_gap = max_gap
        self.lost_reset = lost_reset
        self.states: Dict[int, Estado] = {}
        # Cooldown global da câmera SEPARADO por severidade (mantendo 3s)
        self.last_alert_by_severity = {
            'ATENCAO': float('-inf'),
            'CRITICA': float('-inf'),
        }

    @property
    def last_alert(self) -> float:
        """Compatibilidade: retorna o timestamp do alerta mais recente de qualquer severidade."""
        return max(self.last_alert_by_severity.values())

    @last_alert.setter
    def last_alert(self, value: float):
        self.last_alert_by_severity['CRITICA'] = value
        self.last_alert_by_severity['ATENCAO'] = value

    def get_track_state(self, tid: int) -> Optional[Dict[str, Any]]:
        """Retorna resumo do estado de um track para auditoria em modo development."""
        s = self.states.get(tid)
        if not s:
            return None
        return {
            'streak_atencao': s.streak_atencao,
            'streak_critica': s.streak_critica,
            'normal': s.normal,
            'active_atencao': s.active_atencao,
            'active_critica': s.active_critica,
            'inicio_candidato_atencao_frame': s.candidate_start_atencao_frame,
            'inicio_candidato_critica_frame': s.candidate_start_critica_frame,
            'inicio_candidato_atencao_tempo': s.candidate_start_atencao_time,
            'inicio_candidato_critica_tempo': s.candidate_start_critica_time,
        }

    def update(self, observations: Dict[int, Any], frame_number: int, now: float) -> List[Decisao]:
        """Processa observações para o frame atual e retorna decisões tomadas.

        observations: {track_id: classe_ou_severidade_ou_bool}.
        now: relógio monotônico (stream) ou tempo nominal em segundos (vídeo).

        Regras:
        - 8 frames consecutivos de desequilíbrio confirmam ATENCAO.
        - 8 frames consecutivos de queda confirmam CRITICA.
        - Frames de desequilíbrio não contam como queda; transição inicia nova contagem.
        - Ausência, normal, mudança de classe e lacunas interrompem explicitamente a sequência.
        - IDs não compartilham contagens.
        - Cooldown é separado por severidade (ATENCAO não bloqueia CRITICA).
        - Escalada de ATENCAO para CRITICA no mesmo episódio é permitida.
        - Não há nova emissão de ATENCAO após CRITICA antes do rearmamento normal.
        """
        decisions: List[Decisao] = []

        # 1. Limpeza de tracks perdidos e interrupção por ausência no frame atual
        for tid, s in list(self.states.items()):
            if now - s.last_seen > self.lost_reset:
                del self.states[tid]
            elif tid not in observations:
                # Ausência no frame interrompe sequências em andamento
                s.streak_atencao = 0
                s.streak_critica = 0
                s.normal = 0
                s.candidate_start_atencao_time = None
                s.candidate_start_atencao_frame = None
                s.candidate_start_critica_time = None
                s.candidate_start_critica_frame = None

        # 2. Processa cada track observado no frame atual
        for tid, raw_obs in observations.items():
            obs_type = resolve_observation(raw_obs)
            s = self.states.setdefault(tid, Estado())

            consecutive = (s.last_frame == frame_number - 1 and
                           0 <= now - s.last_seen <= self.max_gap)
            if not consecutive:
                # Lacuna temporal ou salto de frame interrompe contagens
                s.streak_atencao = 0
                s.streak_critica = 0
                s.normal = 0
                s.candidate_start_atencao_time = None
                s.candidate_start_atencao_frame = None
                s.candidate_start_critica_time = None
                s.candidate_start_critica_frame = None

            s.last_seen = now
            s.last_frame = frame_number

            if obs_type == 'NORMAL':
                # Normal interrompe sequências de risco
                s.streak_atencao = 0
                s.streak_critica = 0
                s.candidate_start_atencao_time = None
                s.candidate_start_atencao_frame = None
                s.candidate_start_critica_time = None
                s.candidate_start_critica_frame = None
                s.normal += 1
                if s.normal >= self.normal_frames:
                    # Rearmamento completo do episódio para o track
                    s.active_atencao = False
                    s.active_critica = False

            elif obs_type == 'ATENCAO':
                s.normal = 0
                # Mudança de classe interrompe sequência de queda
                s.streak_critica = 0
                s.candidate_start_critica_time = None
                s.candidate_start_critica_frame = None

                if s.streak_atencao == 0:
                    s.candidate_start_atencao_time = now
                    s.candidate_start_atencao_frame = frame_number

                s.streak_atencao += 1

                if s.streak_atencao >= self.frames:
                    # Evita repetição de ATENCAO no mesmo episódio;
                    # Se já emitiu CRITICA, oscilação para ATENCAO é ignorada até rearmamento.
                    if not s.active_atencao and not s.active_critica:
                        s.active_atencao = True
                        allowed = (now - self.last_alert_by_severity['ATENCAO'] >= self.cooldown)
                        if allowed:
                            self.last_alert_by_severity['ATENCAO'] = now
                        decisions.append(Decisao(
                            track_id=tid,
                            status='confirmado' if allowed else 'suprimido_cooldown',
                            severity='ATENCAO'
                        ))

            elif obs_type == 'CRITICA':
                s.normal = 0
                # Mudança de classe interrompe sequência de atenção
                s.streak_atencao = 0
                s.candidate_start_atencao_time = None
                s.candidate_start_atencao_frame = None

                if s.streak_critica == 0:
                    s.candidate_start_critica_time = now
                    s.candidate_start_critica_frame = frame_number

                s.streak_critica += 1

                if s.streak_critica >= self.frames:
                    # Evita repetição contínua de CRITICA no mesmo track;
                    # Atenção prévia NÃO bloqueia crítica (escalada permitida).
                    if not s.active_critica:
                        s.active_critica = True
                        allowed = (now - self.last_alert_by_severity['CRITICA'] >= self.cooldown)
                        if allowed:
                            self.last_alert_by_severity['CRITICA'] = now
                        decisions.append(Decisao(
                            track_id=tid,
                            status='confirmado' if allowed else 'suprimido_cooldown',
                            severity='CRITICA'
                        ))

        return decisions
