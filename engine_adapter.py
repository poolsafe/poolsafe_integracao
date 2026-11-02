import math
import time
from dataclasses import dataclass

from motor_temporal_v4 import CLASSES, MotorTemporalContinuo


class V4Error(RuntimeError):
    code = 'ERR_V4'


class V4InputError(V4Error):
    code = 'ERR_V4_INPUT'


class V4ResponseError(V4Error):
    code = 'ERR_V4_RESPONSE'


class V4ExecutionError(V4Error):
    code = 'ERR_V4_EXECUTION'


class ModelCompatibilityError(ValueError):
    def __init__(self, code, message, found=None, expected=None):
        super().__init__(f'{code}: {message}')
        self.code, self.message = code, message
        self.found, self.expected = sorted(found or []), sorted(expected or CLASSES)


def validate_model_classes(names):
    if isinstance(names, (list, tuple)):
        names = dict(enumerate(names))
    if not isinstance(names, dict):
        raise ModelCompatibilityError('ERR_MODEL_NAMES', 'Mapeamento de classes inválido')
    try:
        normalized = {int(k): str(v).strip().lower() for k, v in names.items()}
    except (TypeError, ValueError) as exc:
        raise ModelCompatibilityError('ERR_MODEL_NAMES', 'Mapeamento de classes inválido') from exc
    if len(normalized) != 3 or set(normalized) != {0, 1, 2} or set(normalized.values()) != CLASSES:
        raise ModelCompatibilityError('ERR_MODEL_V4_CLASSES', 'V4 exige três classes distintas', list(normalized.values()))
    return normalized


def detections_from_result(result, names):
    try:
        boxes = result.boxes
        if boxes is None or len(boxes) == 0:
            return []
        xyxy = boxes.xyxy.cpu().tolist()
        classes = boxes.cls.cpu().tolist()
        scores = boxes.conf.cpu().tolist()
        ids = boxes.id.cpu().tolist() if boxes.id is not None else [None] * len(boxes)
        if not all(len(values) == len(boxes) for values in (xyxy, classes, scores, ids)):
            raise ValueError('Dimensões de saída inconsistentes')
        detections = []
        for bounds, cls, score, tid in zip(xyxy, classes, scores, ids):
            if not math.isfinite(cls) or int(cls) != cls or int(cls) not in names:
                raise ValueError('ID de classe inválido')
            if tid is not None and (not math.isfinite(tid) or int(tid) != tid):
                raise ValueError('ID de tracking inválido')
            detections.append(dict(track_id=None if tid is None else int(tid),
                                   classe=names[int(cls)], confianca=score, bbox_xyxy=bounds))
        return detections
    except (AttributeError, KeyError, ValueError, TypeError, OverflowError) as exc:
        raise V4InputError('Saída YOLO incompatível com a entrada V4') from exc


@dataclass(frozen=True)
class DecisaoV4:
    track_id: int
    status: str
    severity: str
    event: dict


class MotorV4:
    """Adaptador local; todas as decisões temporais pertencem ao núcleo V4."""

    def __init__(self, cfg, fps=None):
        if not __debug__:
            raise V4InputError('O núcleo de referência requer Python sem -O')
        rate = cfg.engine_fps if fps is None else fps
        if not math.isfinite(rate) or rate <= 0:
            raise V4InputError('FPS do contexto inválido')
        try:
            self.engine = MotorTemporalContinuo(
                confidencias={'pessoa_normal': cfg.conf_normal, 'pessoa_desequilibrio': cfg.conf_desequilibrio,
                              'pessoa_queda': cfg.conf_queda},
                frames_por_classe={'pessoa_desequilibrio': cfg.frames_desequilibrio, 'pessoa_queda': cfg.frames_queda},
                classes_evento={'pessoa_queda', 'pessoa_desequilibrio'} if cfg.emit_attention else {'pessoa_queda'},
                normal_frames=cfg.normal_frames, cooldown=cfg.cooldown, fps=rate,
                camera_id=cfg.codigo_integracao or cfg.camera_id, permitir_escalada=True,
                deduplicar_quedas=True, dedup_iou=cfg.dedup_iou, dedup_intervalo_s=cfg.dedup_interval,
                tolerancia_queda=cfg.fall_tolerance, handoff=cfg.handoff,
                handoff_iou=cfg.handoff_iou, handoff_intervalo_s=cfg.handoff_interval)
        except (AssertionError, ValueError, TypeError) as exc:
            raise V4InputError('Política V4 inválida') from exc
        self.stream_mode = cfg.source_mode == 'stream'
        self.continuity_gap = cfg.continuity_gap
        if not math.isfinite(self.continuity_gap) or self.continuity_gap <= 0:
            raise V4InputError('Limite de continuidade inválido')
        self.last_index = 0
        self.last_time = None
        self.rows, self.transitions, self.continuity = [], [], []
        self.pending_cooldown = set()
        self.duration_s = 0.0
        self.failed = False

    @staticmethod
    def format_input(detections):
        if not isinstance(detections, list):
            raise V4InputError('Detecções devem ser uma lista')
        formatted = []
        try:
            for d in detections:
                tid, label = d['track_id'], d['classe']
                confidence = d['confianca']
                bounds = d['bbox_xyxy']
                if tid is not None and (isinstance(tid, bool) or not isinstance(tid, int) or tid < 0):
                    raise ValueError('track_id')
                if label not in CLASSES or isinstance(confidence, bool):
                    raise ValueError('classe/confiança')
                confidence = float(confidence)
                if not math.isfinite(confidence) or not 0 <= confidence <= 1:
                    raise ValueError('confiança')
                if not isinstance(bounds, (list, tuple)) or len(bounds) != 4:
                    raise ValueError('bbox')
                bounds = tuple(float(v) for v in bounds)
                if not all(math.isfinite(v) for v in bounds) or bounds[2] <= bounds[0] or bounds[3] <= bounds[1]:
                    raise ValueError('bbox')
                formatted.append(dict(track_id=tid, classe=label, confianca=confidence,
                                      x1=bounds[0], y1=bounds[1], x2=bounds[2], y2=bounds[3]))
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise V4InputError('Detecção incompatível com o contrato V4') from exc
        return formatted

    def _validate_response(self, rows, events, index, raw):
        if not isinstance(rows, list) or len(rows) != len(raw):
            raise V4ResponseError('Quantidade de linhas incompatível')
        emitted = set()
        try:
            for row, item in zip(rows, raw):
                if any(row[key] != item[key] for key in ('track_id', 'classe', 'confianca')):
                    raise V4ResponseError('Identidade de resposta incompatível')
                if not all(isinstance(row[key], bool) for key in ('aceita', 'evento_emitido', 'suprimido_cooldown')):
                    raise V4ResponseError('Flags V4 inválidas')
                if row['evento_emitido']:
                    emitted.add((row['track_id'], row['classe']))
            seen = set()
            for event in events:
                key = (event['track_id'], event['classe_origem'])
                if event['frame'] != index or key not in emitted or key in seen:
                    raise V4ResponseError('Evento V4 sem linha correspondente ou repetido')
                if event['classe_origem'] not in {'pessoa_queda', 'pessoa_desequilibrio'}:
                    raise V4ResponseError('Classe de evento inválida')
                if event['tipo_evento'] not in {'abertura_episodio', 'elevacao_gravidade'}:
                    raise V4ResponseError('Tipo de evento inválido')
                if not isinstance(event['evento_referencia'], str) or not event['evento_referencia']:
                    raise V4ResponseError('Referência de episódio ausente')
                if not math.isfinite(event['confidence']) or not 0 <= event['confidence'] <= 1:
                    raise V4ResponseError('Confiança de evento inválida')
                seen.add(key)
            if seen != emitted:
                raise V4ResponseError('Linha emissiva sem evento')
        except (KeyError, TypeError, ValueError) as exc:
            raise V4ResponseError('Resposta V4 incompleta') from exc

    def update_detections(self, detections, frame_number, now):
        if self.failed:
            raise V4ExecutionError('Instância V4 inválida após falha; reinicie o segmento')
        if isinstance(frame_number, bool) or not isinstance(frame_number, int) or frame_number <= self.last_index:
            raise V4InputError('Frame não crescente')
        if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now):
            raise V4InputError('Timestamp inválido')
        if self.last_time is not None and now < self.last_time:
            raise V4InputError('Timestamp regressivo')
        raw = self.format_input(detections)
        started = time.perf_counter()
        try:
            # now é monotônico em stream; vídeo mantém seu tempo nominal.
            if self.stream_mode and self.last_time is not None and now-self.last_time > self.continuity_gap:
                for st in self.engine.states.values():
                    self.engine.clear_risk(st)
                    st['normal'] = 0
                self.engine.evidencias_queda.clear()
                self.engine.ultima_caixa.clear()
                self.engine.ultima_presenca.clear()
                self.engine.ultima_classe_aceita.clear()
                self.pending_cooldown.clear()
                self.engine.auditoria_continuidade.append(dict(
                    frame=frame_number, tempo_video_s=now,
                    transicao='interrupcao_lacuna_stream', intervalo_s=now-self.last_time,
                    limite_s=self.continuity_gap))
            rows = self.engine.step(frame_number, now, raw)
            events = list(self.engine.events)
            self._validate_response(rows, events, frame_number, raw)
            self.rows = rows
            self.transitions = list(self.engine.transitions)
            self.continuity = list(self.engine.auditoria_continuidade)
            suppressed = {(r['track_id'], r['classe']) for r in rows if r['suprimido_cooldown']}
            decisions = [DecisaoV4(tid, 'suprimido_cooldown',
                                  'CRITICA' if cls == 'pessoa_queda' else 'ATENCAO', {})
                         for tid, cls in sorted(suppressed - self.pending_cooldown)]
            decisions.extend(DecisaoV4(e['track_id'], 'confirmado',
                                      'CRITICA' if e['classe_origem'] == 'pessoa_queda' else 'ATENCAO', dict(e))
                             for e in events)
            self.pending_cooldown = suppressed
            self.last_index, self.last_time = frame_number, now
            self.engine.events.clear()
            self.engine.transitions.clear()
            self.engine.auditoria_continuidade.clear()
            return decisions
        except V4ResponseError:
            self.failed = True
            raise
        except Exception as exc:
            self.failed = True
            raise V4ExecutionError('Falha no núcleo V4; nenhuma resposta deste passo foi publicada') from exc
        finally:
            self.duration_s = time.perf_counter() - started

    def get_track_state(self, tid):
        s = self.engine.states.get(tid)
        if s is None:
            return None
        return dict(streak_atencao=s['counts']['pessoa_desequilibrio'],
                    streak_critica=s['counts']['pessoa_queda'], normal=s['normal'],
                    active_atencao=s['active'] and s['severity'] == 'pessoa_desequilibrio',
                    active_critica=s['active'] and s['severity'] == 'pessoa_queda',
                    episodio=s['episode'], evento_referencia=s['event_ref'])
