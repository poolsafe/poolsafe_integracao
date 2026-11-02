"""Auditoria auxiliar: nunca participa das decisões nem publica registros de frames."""
from copy import deepcopy
from importlib.metadata import version, PackageNotFoundError
import json
import logging
from pathlib import Path

from engine_adapter import detections_from_result
from observabilidade import log_event


def temporal_snapshot(motor):
    engine = motor.engine
    return {str(tid): dict(
        estado=deepcopy(st),
        evidencias_queda=[list(item) for item in engine.evidencias_queda.get(tid, [])])
        for tid, st in engine.states.items()}


class FrameDiagnostics:
    def __init__(self, cfg, run_dir, run_id):
        self.enabled = cfg.log_mode == 'homologation' and cfg.homologation_diagnostics_enabled
        self.limit = cfg.homologation_diagnostics_max_frames
        self.path = Path(run_dir) / 'diagnostico_frames.jsonl'
        self.run_id, self.count, self.attempted = run_id, 0, 0
        self.stopped = False
        self.warned = set()
        self.pre_tracker = None
        self.postprocess_filters = None
        self.stage_status = 'nao_observado'
        self.installed = False
        try:
            self.ultralytics_version = version('ultralytics')
        except PackageNotFoundError:
            self.ultralytics_version = None

    @property
    def active(self):
        return self.enabled and not self.stopped

    def warn(self, code, **fields):
        if code not in self.warned:
            self.warned.add(code)
            log_event(logging.getLogger('poolsafe.diagnostico'), 'diagnostics.warning',
                      level=logging.WARNING, code=code, **fields)

    def install(self, model, names):
        if not self.active:
            return
        if self.ultralytics_version != '8.4.173':
            self.stage_status = 'indisponivel_versao_nao_validada'
            self.warn(self.stage_status, ultralytics_version=self.ultralytics_version)
            return
        # O model recém-criado ainda não registrou o tracker. Apenas acrescenta
        # nosso observador; nunca envolve, remove ou reordena callbacks existentes.
        try:
            callbacks = model.callbacks['on_predict_postprocess_end']
            if any(getattr(getattr(cb, 'func', cb), '__module__', '').startswith('ultralytics.trackers')
                   for cb in callbacks):
                self.stage_status = 'indisponivel_tracker_ja_registrado'
                self.warn(self.stage_status)
                return
            model.add_callback('on_predict_postprocess_end', self._observe(names))
            self.installed = True
        except Exception as exc:
            self.stage_status = 'indisponivel_instalacao'
            self.warn(self.stage_status, error_type=type(exc).__name__)

    def _observe(self, names):
        def observe(predictor):
            if not self.active:
                return
            try:
                if len(predictor.results) != 1:
                    raise ValueError('Auditoria requer um frame por chamada')
                # tolist + deepcopy: nenhum tensor/caixa do Results é retido.
                self.pre_tracker = deepcopy(detections_from_result(predictor.results[0], names))
                args = getattr(predictor, 'args', None)
                self.postprocess_filters = {key: deepcopy(getattr(args, key, None))
                                            for key in ('conf', 'iou', 'classes', 'agnostic_nms', 'max_det')}
                self.stage_status = 'observado_pos_nms_antes_tracker'
            except Exception as exc:
                self.stage_status = 'indisponivel_observacao'
                self.warn(self.stage_status, error_type=type(exc).__name__)
        return observe

    def begin(self, motor):
        self.pre_tracker = None
        self.postprocess_filters = None
        if self.installed:
            self.stage_status = 'callback_nao_executado'
        if not self.active:
            return None
        try:
            return temporal_snapshot(motor)
        except Exception as exc:
            self.warn('snapshot_failed', error_type=type(exc).__name__)
            return None

    def write(self, *, motor, before, detections, decisions, segment, processed_frame,
              captured_frame, observed_at, logical_time, source_mode, inference_conf, iou):
        if not self.active:
            return
        self.attempted += 1
        try:
            after = temporal_snapshot(motor)
            # A ausência é observável; não se atribui a causa a oclusão/NMS/tracker.
            present = {str(d['track_id']) for d in detections if d['track_id'] is not None}
            record = dict(
                run_id=self.run_id, segmento=segment, frame_processado=processed_frame,
                frame_capturado=captured_frame, observado_em=observed_at,
                referencia_temporal=dict(tipo='monotonico_captura' if source_mode == 'stream' else 'video_nominal',
                                         valor_s=logical_time),
                etapas=dict(ultralytics_version=self.ultralytics_version, status=self.stage_status,
                            detector_pos_nms=self.pre_tracker, apos_tracker=detections,
                            parametros_posprocessamento=self.postprocess_filters,
                            filtros=dict(conf_inferencia=inference_conf, iou_nms=iou)),
                sem_deteccao=not detections,
                deteccoes_sem_id=sum(d['track_id'] is None for d in detections),
                observacoes=motor.rows, estado_antes=before, estado_depois=after,
                ids_ausentes=[tid for tid in (before or {}) if tid not in present],
                transicoes=motor.transitions, continuidade=motor.continuity,
                decisoes=[dict(track_id=d.track_id, status=d.status, severity=d.severity, event=d.event)
                          for d in decisions])
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + '\n')
            self.count += 1
        except Exception as exc:
            # Encerra a coleta após falha: evita JSONL posterior a uma linha parcial.
            self.stopped = True
            self.warn('write_failed', error_type=type(exc).__name__)
        if self.attempted >= self.limit:
            self.stopped = True
            self.warn('frame_limit_reached', max_frames=self.limit)

    def summary(self):
        return dict(enabled=self.enabled, max_frames=self.limit, frames_written=self.count,
                    frames_attempted=self.attempted, stopped=self.stopped,
                    warnings=sorted(self.warned), arquivo=self.path.name,
                    ultralytics_version=self.ultralytics_version, stage_status=self.stage_status)
