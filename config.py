"""Configuração por ambiente: nenhum endereço ou segredo real pertence ao código."""
import os
import math
from dataclasses import dataclass
from pathlib import Path


def flag(name, default=False):
    value = os.getenv(name, str(default)).lower()
    if value not in {'true', 'false', '1', '0'}:
        raise ValueError(f'{name}: use true ou false')
    return value in {'true', '1'}


@dataclass
class Config:
    # Uma câmera por processo. Para outra câmera, suba outro serviço/contêiner.
    camera_id: str = os.getenv('CAMERA_ID', 'camera_teste_01')
    codigo_integracao: str = os.getenv('CODIGO_INTEGRACAO', '')
    source_mode: str = os.getenv('SOURCE_MODE', 'video')
    source: str = os.getenv('SOURCE', '/entrada/teste.mp4')
    model_path: str = os.getenv('MODEL_PATH', '/modelos/best.pt')
    model_version: str = os.getenv('MODEL_VERSION', 'candidato')
    model_sha256: str = os.getenv('MODEL_SHA256', '')
    device: str = os.getenv('DEVICE', 'cpu')
    engine_version: str = os.getenv('ENGINE_VERSION', 'v4')
    engine_fps: float = float(os.getenv('ENGINE_FPS', '15'))
    torch_threads: int = int(os.getenv('TORCH_THREADS', '2'))
    torch_interop_threads: int = int(os.getenv('TORCH_INTEROP_THREADS', '1'))
    opencv_threads: int = int(os.getenv('OPENCV_THREADS', '1'))
    metrics_window: int = int(os.getenv('METRICS_WINDOW', '1024'))
    metrics_interval: float = float(os.getenv('METRICS_INTERVAL_S', '15'))
    health_stale: float = float(os.getenv('HEALTH_STALE_S', '90'))
    v4_log_every: int = int(os.getenv('V4_LOG_EVERY_N_FRAMES', '150'))
    conf_normal: float = float(os.getenv('CONF_NORMAL', '0.30'))
    conf_desequilibrio: float = float(os.getenv('CONF_DESEQUILIBRIO', '0.65'))
    conf_queda: float = float(os.getenv('CONF_QUEDA', '0.50'))
    frames_desequilibrio: int = int(os.getenv('FRAMES_DESEQUILIBRIO', '15'))
    frames_queda: int = int(os.getenv('EVIDENCIAS_QUEDA', '10'))
    fall_tolerance: int = int(os.getenv('TOLERANCIA_QUEDA_FRAMES', '1'))
    dedup_iou: float = float(os.getenv('DEDUP_IOU', '0.50'))
    dedup_interval: float = float(os.getenv('DEDUP_INTERVALO_S', '1.5'))
    handoff: bool = flag('HANDOFF_ENABLED', True)
    handoff_iou: float = float(os.getenv('HANDOFF_IOU', '0.70'))
    handoff_interval: float = float(os.getenv('HANDOFF_INTERVALO_S', '0.30'))
    emit_attention: bool = flag('EMIT_ATTENTION', True)
    iou: float = float(os.getenv('IOU', '0.70'))
    imgsz: int = int(os.getenv('IMGSZ', '640'))
    risk_class: str = os.getenv('RISK_CLASS', 'pessoa_queda')
    normal_class: str = os.getenv('NORMAL_CLASS', 'pessoa_normal')
    desequilibrio_class: str = os.getenv('DESEQUILIBRIO_CLASS', 'pessoa_desequilibrio')
    log_mode: str = os.getenv('LOG_MODE', 'production').lower()
    homologation_log_every: int = int(os.getenv('HOMOLOGATION_LOG_EVERY_N_FRAMES', '100'))
    captures_enabled: bool = flag('CAPTURES_ENABLED') if os.getenv('CAPTURES_ENABLED', '') else None
    captures_interval: float = float(os.getenv('CAPTURES_INTERVAL_S', '1'))
    captures_max_count: int = int(os.getenv('CAPTURES_MAX_COUNT', '1000'))
    captures_max_bytes: int = int(os.getenv('CAPTURES_MAX_BYTES', '524288000'))
    normal_frames: int = int(os.getenv('FRAMES_NORMAIS_ENCERRAR', '15'))
    cooldown: float = float(os.getenv('COOLDOWN_S', '3'))
    continuity_gap: float = float(os.getenv('CONTINUIDADE_MAX_INTERVALO_S', '2.0'))
    max_gap: float = float(os.getenv('MAX_INTERVALO_S', '0.5'))
    heartbeat_interval: float = float(os.getenv('HEARTBEAT_INTERVAL_S', '30'))
    heartbeat_ttl: float = float(os.getenv('HEARTBEAT_TTL_S', '60'))
    alert_ttl: float = float(os.getenv('ALERT_TTL_S', '0'))
    test: bool = flag('TEST_MODE', True)
    allow_test_publish: bool = flag('ALLOW_TEST_PUBLISH', False)
    publish: bool = flag('PUBLISH_RABBIT', False)
    evidence: bool = flag('INCLUDE_EVIDENCE', False)
    data_dir: str = os.getenv('DATA_DIR', '/dados')
    max_pending: int = int(os.getenv('MAX_PENDING_EVENTS', '10000'))
    drain_seconds: float = float(os.getenv('SHUTDOWN_DRAIN_S', '15'))
    rabbit_host: str = os.getenv('RABBIT_HOST', 'rabbitmq')
    rabbit_port: int = int(os.getenv('RABBIT_PORT', '5672'))
    rabbit_vhost: str = os.getenv('RABBIT_VHOST', 'poolsafe')
    rabbit_user: str = os.getenv('RABBIT_USER', '')
    rabbit_password: str = os.getenv('RABBIT_PASSWORD', '')
    rabbit_tls: bool = flag('RABBIT_TLS', False)
    rabbit_ca: str = os.getenv('RABBIT_CA_FILE', '')
    exchange: str = os.getenv('RABBIT_EXCHANGE', 'poolsafe.events')
    alert_key: str = os.getenv('RABBIT_ALERT_KEY', 'risk.detected')
    heartbeat_key: str = os.getenv('RABBIT_HEARTBEAT_KEY', 'camera.heartbeat')

    def __post_init__(self):
        if self.captures_enabled is None:
            self.captures_enabled = self.log_mode == 'homologation'

    @property
    def inference_conf(self):
        return min(self.conf_normal, self.conf_desequilibrio, self.conf_queda)

    def engine_policy(self):
        return {
            'engine_version': 'v4',
            'confidencias': {'pessoa_normal': self.conf_normal, 'pessoa_desequilibrio': self.conf_desequilibrio, 'pessoa_queda': self.conf_queda},
            'frames_desequilibrio': self.frames_desequilibrio,
            'evidencias_queda': self.frames_queda, 'tolerancia_queda_frames': self.fall_tolerance,
            'normal_frames': self.normal_frames, 'cooldown_s': self.cooldown,
            'cooldown_policy': 'camera_critica_independente_de_atencao_elevacao_livre',
            'dedup_iou': self.dedup_iou, 'dedup_intervalo_s': self.dedup_interval,
            'handoff': self.handoff, 'handoff_iou': self.handoff_iou,
            'handoff_intervalo_s': self.handoff_interval, 'engine_fps': self.engine_fps,
            'continuidade_max_intervalo_s': self.continuity_gap,
            'continuidade_policy': 'stream_monotonico_limpa_pendencias_preserva_episodios',
            'stream_sampling': 'latest_decoded_frame',
            'emit_attention': self.emit_attention,
        }

    def validate(self):
        if self.engine_version != 'v4':
            raise ValueError('Somente ENGINE_VERSION=v4 é suportado')
        if min(self.torch_threads, self.torch_interop_threads, self.metrics_window, self.v4_log_every) < 1 or self.opencv_threads < 0:
            raise ValueError('Parâmetros de recursos inválidos')
        for value in (self.engine_fps, self.metrics_interval, self.health_stale, self.max_gap, self.continuity_gap):
            if not math.isfinite(value) or value <= 0:
                raise ValueError('Intervalos de execução inválidos')
        if self.log_mode not in {'development', 'production', 'homologation'}:
            raise ValueError('LOG_MODE deve ser development, production ou homologation')
        if min(self.homologation_log_every, self.captures_max_count, self.captures_max_bytes) < 1:
            raise ValueError('Limites de observabilidade devem ser positivos')
        if not math.isfinite(self.captures_interval) or self.captures_interval <= 0:
            raise ValueError('CAPTURES_INTERVAL_S deve ser positivo e finito')
        if self.source_mode not in {'video', 'stream'}:
            raise ValueError('SOURCE_MODE deve ser video ou stream')
        if not self.camera_id or not self.source:
            raise ValueError('CAMERA_ID e SOURCE são obrigatórios')
        if not Path(self.model_path).is_file():
            raise ValueError('MODEL_PATH não aponta para arquivo existente')
        if self.source_mode == 'video' and not Path(self.source).is_file():
            raise ValueError('Vídeo SOURCE inexistente dentro do contêiner')
        if not math.isfinite(self.iou) or not 0 < self.iou <= 1:
            raise ValueError('IOU deve estar entre 0 e 1')
        if self.model_sha256 and (len(self.model_sha256) != 64 or any(x not in '0123456789abcdef' for x in self.model_sha256.lower())):
            raise ValueError('MODEL_SHA256 inválido')
        if min(self.imgsz, self.max_pending) < 1:
            raise ValueError('Contagens e resolução precisam ser positivas')
        if self.alert_ttl < 0:
            raise ValueError('ALERT_TTL_S deve ser maior ou igual a zero')
        if min(self.max_gap, self.heartbeat_interval,
               self.heartbeat_ttl, self.drain_seconds) <= 0 or self.cooldown < 0:
            raise ValueError('Intervalos inválidos')
        if self.heartbeat_interval >= self.heartbeat_ttl:
            raise ValueError('TTL do heartbeat precisa superar seu intervalo')
        if self.publish and not self.codigo_integracao:
            raise ValueError('CODIGO_INTEGRACAO é obrigatório quando a publicação estiver habilitada')
        if self.publish and self.test and not self.allow_test_publish:
            raise ValueError(
                'Publicação em modo de teste exige liberação explícita (ALLOW_TEST_PUBLISH=true). '
                'Essa configuração requer destino, workflow e câmera com destinatários controlados.'
            )
        if self.publish and not (self.rabbit_user and self.rabbit_password):
            raise ValueError('Informe RABBIT_USER e RABBIT_PASSWORD')
        if not self.test and not self.codigo_integracao:
            raise ValueError('Fora de teste informe CODIGO_INTEGRACAO cadastrado no domínio')
