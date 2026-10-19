"""Configuração por ambiente: nenhum endereço ou segredo real pertence ao código."""
import os
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
    device: str = os.getenv('DEVICE', 'cpu')
    conf: float = float(os.getenv('CONF', '0.25'))
    iou: float = float(os.getenv('IOU', '0.70'))
    imgsz: int = int(os.getenv('IMGSZ', '640'))
    risk_class: str = os.getenv('RISK_CLASS', 'pessoa_queda')
    normal_class: str = os.getenv('NORMAL_CLASS', 'pessoa_normal')
    frames: int = int(os.getenv('FRAMES_PARA_ALERTA', '8'))
    normal_frames: int = int(os.getenv('FRAMES_NORMAIS_ENCERRAR', '15'))
    cooldown: float = float(os.getenv('COOLDOWN_S', '3'))
    max_gap: float = float(os.getenv('MAX_INTERVALO_S', '0.5'))
    lost_reset: float = float(os.getenv('LOST_RESET_S', '2'))
    heartbeat_interval: float = float(os.getenv('HEARTBEAT_INTERVAL_S', '30'))
    heartbeat_ttl: float = float(os.getenv('HEARTBEAT_TTL_S', '60'))
    alert_ttl: float = float(os.getenv('ALERT_TTL_S', '120'))
    test: bool = flag('TEST_MODE', True)
    publish: bool = flag('PUBLISH_RABBIT', False)
    evidence: bool = flag('INCLUDE_EVIDENCE', False)
    data_dir: str = os.getenv('DATA_DIR', '/dados')
    max_pending: int = int(os.getenv('MAX_PENDING_EVENTS', '10000'))
    drain_seconds: float = float(os.getenv('SHUTDOWN_DRAIN_S', '15'))
    rabbit_host: str = os.getenv('RABBIT_HOST', 'rabbitmq')
    rabbit_port: int = int(os.getenv('RABBIT_PORT', '5672'))
    rabbit_vhost: str = os.getenv('RABBIT_VHOST', '/')
    rabbit_user: str = os.getenv('RABBIT_USER', '')
    rabbit_password: str = os.getenv('RABBIT_PASSWORD', '')
    rabbit_tls: bool = flag('RABBIT_TLS', False)
    rabbit_ca: str = os.getenv('RABBIT_CA_FILE', '')
    exchange: str = os.getenv('RABBIT_EXCHANGE', 'poolsafe.eventos.v1')
    alert_queue: str = os.getenv('RABBIT_ALERT_QUEUE', 'poolsafe.alertas.v1')
    heartbeat_queue: str = os.getenv('RABBIT_HEARTBEAT_QUEUE', 'poolsafe.heartbeat.v1')
    alert_key: str = os.getenv('RABBIT_ALERT_KEY', 'poolsafe.alerta')
    heartbeat_key: str = os.getenv('RABBIT_HEARTBEAT_KEY', 'poolsafe.heartbeat')

    def validate(self):
        if self.source_mode not in {'video', 'stream'}:
            raise ValueError('SOURCE_MODE deve ser video ou stream')
        if not self.camera_id or not self.source:
            raise ValueError('CAMERA_ID e SOURCE são obrigatórios')
        if not Path(self.model_path).is_file():
            raise ValueError('MODEL_PATH não aponta para arquivo existente')
        if self.source_mode == 'video' and not Path(self.source).is_file():
            raise ValueError('Vídeo SOURCE inexistente dentro do contêiner')
        if not 0 < self.conf <= 1 or not 0 < self.iou <= 1:
            raise ValueError('CONF e IOU devem estar entre 0 e 1')
        if min(self.frames, self.normal_frames, self.imgsz, self.max_pending) < 1:
            raise ValueError('Contagens e resolução precisam ser positivas')
        if min(self.max_gap, self.lost_reset, self.heartbeat_interval,
               self.heartbeat_ttl, self.alert_ttl, self.drain_seconds) <= 0 or self.cooldown < 0:
            raise ValueError('Intervalos inválidos')
        if self.heartbeat_interval >= self.heartbeat_ttl:
            raise ValueError('TTL do heartbeat precisa superar seu intervalo')
        if self.publish and not (self.rabbit_user and self.rabbit_password):
            raise ValueError('Informe RABBIT_USER e RABBIT_PASSWORD')
        if not self.test and not self.codigo_integracao:
            raise ValueError('Fora de teste informe CODIGO_INTEGRACAO cadastrado no domínio')
