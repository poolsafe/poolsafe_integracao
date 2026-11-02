import math
from collections import Counter, deque

CLASSES = {'pessoa_normal', 'pessoa_desequilibrio', 'pessoa_queda'}
CLASSES_RISCO = ('pessoa_desequilibrio', 'pessoa_queda')

class MotorTemporal:
    """Contagem por ID e por classe, independente da leitura de vídeo.

    Contadores exigem frames consecutivos da MESMA classe, acima do limiar.
    Ausência, baixa confiança e mudança de classe quebram a sequência.
    Somente normal aceito por normal_frames consecutivos encerra o episódio.
        Um episódio emite abertura e, opcionalmente, uma elevação para queda.
        permitir_escalada=False reproduz a política anterior.
        A elevação confirmada ignora o cooldown, mas não repete a mesma gravidade.
    O cooldown é por câmera; atenção não bloqueia crítica, que consulta
    somente a última crítica. Se bloquear, tenta emitir nos próximos frames
    somente enquanto a mesma condição continua confirmada.
    """

    def __init__(self, confidencias, frames_por_classe, classes_evento,
                 normal_frames=15, cooldown=3.0, fps=15.0, camera_id='validacao_01',
                 permitir_escalada=True, deduplicar_quedas=False,
                 dedup_iou=0.50, dedup_intervalo_s=1.5):
        assert set(confidencias) == CLASSES, 'Confira os três nomes de classe.'
        assert all(math.isfinite(v) and 0 < v <= 1 for v in confidencias.values())
        assert set(frames_por_classe) == set(CLASSES_RISCO)
        assert all(isinstance(v, int) and not isinstance(v, bool) and v >= 1
                   for v in frames_por_classe.values())
        assert set(classes_evento) <= set(CLASSES_RISCO)
        assert isinstance(normal_frames, int) and normal_frames >= 1
        assert math.isfinite(cooldown) and cooldown >= 0 and fps > 0
        self.confidencias = dict(confidencias)
        self.frames_por_classe = dict(frames_por_classe)
        self.classes_evento = set(classes_evento)
        self.normal_frames, self.cooldown, self.fps = normal_frames, cooldown, fps
        self.camera_id = camera_id
        self.permitir_escalada = permitir_escalada
        assert 0 < dedup_iou <= 1 and dedup_intervalo_s > 0
        self.deduplicar_quedas = deduplicar_quedas
        self.dedup_iou, self.dedup_intervalo_s = dedup_iou, dedup_intervalo_s
        self.states, self.events = {}, []
        self.transitions = []
        self.last_event = float('-inf')
        self.last_critical = float('-inf')

    @staticmethod
    def new_state():
        return dict(counts={c: 0 for c in CLASSES_RISCO}, normal=0, active=False,
                    last=-1, starts={c: None for c in CLASSES_RISCO},
                    confirmed_at={c: None for c in CLASSES_RISCO}, episode=0, severity=None,
                    fall_box=None, fall_s=None, event_ref=None, associated=False)

    @staticmethod
    def clear_risk(st, only=None):
        for c in ([only] if only else CLASSES_RISCO):
            st['counts'][c] = 0
            st['starts'][c] = st['confirmed_at'][c] = None

    @staticmethod
    def box_iou(a, b):
        ix = max(0.0, min(a[2], b[2])-max(a[0], b[0]))
        iy = max(0.0, min(a[3], b[3])-max(a[1], b[1]))
        inter = ix*iy
        area_a = max(0.0, a[2]-a[0])*max(0.0, a[3]-a[1])
        area_b = max(0.0, b[2]-b[0])*max(0.0, b[3]-b[1])
        return inter/max(area_a+area_b-inter, 1e-12)

    def duplicate_fall(self, tid, box, video_s, snapshots):
        """Associação de EVENTO, sem alterar IDs ou transferir contadores.

        Só compara quedas aceitas recentes de episódios já confirmados.
        Candidatos de uma mesma referência contam como uma ocorrência.
        Se houver mais de uma ocorrência possível, não suprime por associação.
        """
        if not self.deduplicar_quedas:
            return None, False
        candidates = {}
        for other_tid, st in snapshots.items():
            if other_tid == tid or not st['active'] or st['severity'] != 'pessoa_queda':
                continue
            if st['fall_box'] is None or st['fall_s'] is None or not st['event_ref']:
                continue
            gap = video_s-st['fall_s']
            if not 0 <= gap <= self.dedup_intervalo_s:
                continue
            overlap = self.box_iou(box, st['fall_box'])
            if overlap < self.dedup_iou:
                continue
            item = dict(track_id=other_tid, referencia=st['event_ref'], iou=overlap,
                        intervalo_s=gap)
            root = st['event_ref']
            if root not in candidates or overlap > candidates[root]['iou']:
                candidates[root] = item
        return (next(iter(candidates.values())), False) if len(candidates)==1 else (None, len(candidates)>1)

    def step(self, index, video_s, detections, track=True):
        """Recebe todas as caixas retornadas pelo modo e devolve linhas auditáveis.
        Detecções ausentes do retorno do ByteTrack não podem ser recuperadas aqui.
        """
        rows = []
        for d in detections:
            r = dict(d)
            assert r['classe'] in CLASSES
            r.update(limiar_classe=self.confidencias[r['classe']],
                     aceita=r['confianca'] >= self.confidencias[r['classe']],
                     usada_temporal=False, contador_desequilibrio=0, contador_queda=0,
                     contador_normal=0, episodio_ativo=False, confirmacao_temporal=False,
                     evento_emitido=False, suprimido_cooldown=False,
                     episodio_encerrado=False, gravidade_episodio='',
                     tipo_evento='', episodio=0, evento_referencia='',
                     associado_evento_existente=False, duplicata_associada=False,
                     id_referencia='', iou_associacao='', intervalo_associacao_s='',
                     associacao_ambigua=False, motivo='')
            r['motivo'] = ('sem_motor_no_predict' if r['aceita'] else 'abaixo_limiar')
            rows.append(r)
        if not track:
            return rows

        # Um ID não pode avançar duas vezes no mesmo frame. Prefere caixa aceita;
        # entre caixas aceitas, usa a de maior confiança. As demais ficam no CSV.
        chosen = {}
        for j, r in enumerate(rows):
            tid = r['track_id']
            if tid is None:
                r['motivo'] = 'sem_id' if r['aceita'] else 'abaixo_limiar_sem_id'
                continue
            key = (r['aceita'], r['confianca'])
            if tid not in chosen or key > chosen[tid][0]:
                chosen[tid] = (key, j)
        selected = {j for _, j in chosen.values()}
        for j, r in enumerate(rows):
            if r['track_id'] is not None and j not in selected:
                r['motivo'] = 'duplicata_id_no_frame'

        # Fotografia anterior ao frame: a ordem das caixas não decide associações.
        snapshots = {tid: dict(st) for tid, st in self.states.items()}
        # Atualiza posição da referência com queda aceita deste frame, mas não
        # usa caixa rejeitada como evidência de queda. Sem somar contadores.
        for _, j in chosen.values():
            rr = rows[j]
            ss = snapshots.get(rr['track_id'])
            if ss is not None and rr['aceita'] and rr['classe']=='pessoa_queda':
                ss['fall_box'] = tuple(rr[k] for k in ('x1','y1','x2','y2'))
                ss['fall_s'] = video_s
            elif ss is not None and rr['aceita'] and rr['classe']=='pessoa_normal':
                # Evidência atual de normalidade impede vincular outro ID a uma
                # posição antiga de queda. Não encerra/rearma o episódio aqui.
                ss['fall_box'] = None
        seen = set()
        for _, j in chosen.values():
            r = rows[j]
            tid, cls = r['track_id'], r['classe']
            seen.add(tid)
            st = self.states.setdefault(tid, self.new_state())
            if st['last'] != index - 1:
                self.clear_risk(st)
                st['normal'] = 0
            st['last'] = index
            r['usada_temporal'] = bool(r['aceita'])
            if r['aceita'] and cls == 'pessoa_queda':
                st['fall_box'] = tuple(r[k] for k in ('x1','y1','x2','y2'))
                st['fall_s'] = video_s
            elif r['aceita'] and cls == 'pessoa_normal':
                st['fall_box'] = None

            if not r['aceita']:
                # Rejeitada não é normal e não rearma um episódio ativo.
                self.clear_risk(st)
                st['normal'] = 0
                r['motivo'] = 'abaixo_limiar'
            elif cls == 'pessoa_normal':
                self.clear_risk(st)
                st['normal'] += 1
                r['motivo'] = 'normal_aceito'
                if st['normal'] >= self.normal_frames and st['active']:
                    anterior = st['severity']
                    st['active'] = False
                    st['severity'] = None
                    st['event_ref'] = None
                    st['associated'] = False
                    r['episodio_encerrado'] = True
                    self.transitions.append(dict(frame=index, tempo_video_s=video_s,
                        track_id=tid, episodio=st['episode'],
                        transicao='encerramento_recuperacao', gravidade_anterior=anterior,
                        gravidade_nova=None, contador_normal=st['normal']))
            else:
                st['normal'] = 0
                for other in CLASSES_RISCO:
                    if other != cls:
                        self.clear_risk(st, only=other)
                if st['counts'][cls] == 0:
                    st['starts'][cls] = video_s
                st['counts'][cls] += 1
                r['motivo'] = 'aguardando_persistencia'
                if st['counts'][cls] >= self.frames_por_classe[cls]:
                    r['confirmacao_temporal'] = True
                    if st['confirmed_at'][cls] is None:
                        st['confirmed_at'][cls] = video_s
                    # Uma escalada pertence ao MESMO episódio e exige a sua
                    # própria sequência de queda; não soma classes.
                    escalada = (self.permitir_escalada and st['active']
                        and st['severity'] == 'pessoa_desequilibrio'
                        and cls == 'pessoa_queda')
                    duplicate, ambiguous = (None, False)
                    if cls == 'pessoa_queda' and (not st['active'] or escalada):
                        duplicate, ambiguous = self.duplicate_fall(
                            tid, st['fall_box'], video_s, snapshots)
                    r['associacao_ambigua'] = ambiguous
                    if cls not in self.classes_evento:
                        r['motivo'] = 'classe_sem_evento'
                    elif st['active'] and not escalada:
                        r['motivo'] = 'episodio_ja_ativo'
                    elif duplicate is not None:
                        # Confirmação pertence a uma queda já notificada por outro ID.
                        # Marca o novo ID como associado, sem criar evento e sem
                        # reiniciar cooldown. Se persistir, continua sem repetir.
                        if not st['active']:
                            st['episode'] += 1
                        st['active'] = True
                        st['severity'] = 'pessoa_queda'
                        st['event_ref'] = duplicate['referencia']
                        st['associated'] = True
                        r.update(duplicata_associada=True,
                            id_referencia=duplicate['track_id'],
                            iou_associacao=duplicate['iou'],
                            intervalo_associacao_s=duplicate['intervalo_s'],
                            motivo='queda_associada_evento_existente')
                        self.transitions.append(dict(frame=index, tempo_video_s=video_s,
                            track_id=tid, episodio=st['episode'],
                            transicao='associacao_duplicata_queda',
                            evento_referencia=st['event_ref'],
                            id_referencia=duplicate['track_id'],
                            iou=duplicate['iou'], intervalo_s=duplicate['intervalo_s']))
                    elif not escalada and video_s - (self.last_critical if cls == 'pessoa_queda' else self.last_event) < self.cooldown:
                        r['suprimido_cooldown'] = True
                        r['motivo'] = 'cooldown_aguardar_persistencia'
                        # Não marca ativo: evita perder definitivamente um evento
                        # apenas porque sua primeira confirmação caiu no cooldown.
                    else:
                        anterior = st['severity']
                        if not st['active']:
                            st['episode'] += 1
                        st['active'] = True
                        st['severity'] = cls
                        if not st['event_ref']:
                            st['event_ref'] = f'{self.camera_id}:{tid}:{st["episode"]}'
                        st['associated'] = False
                        tipo = 'elevacao_gravidade' if escalada else 'abertura_episodio'
                        r['tipo_evento'] = tipo
                        self.transitions.append(dict(frame=index, tempo_video_s=video_s,
                            track_id=tid, episodio=st['episode'], transicao=tipo,
                            gravidade_anterior=anterior, gravidade_nova=cls))
                        self.last_event = video_s
                        if cls == 'pessoa_queda':
                            self.last_critical = video_s
                        r['evento_emitido'] = True
                        r['motivo'] = 'evento_local_emitido'
                        self.events.append(dict(camera_id=self.camera_id, track_id=tid,
                            episodio=st['episode'], frame=index, tempo_video_s=round(video_s, 6),
                            tipo_evento=tipo, gravidade_anterior=anterior,
                            evento_referencia=st['event_ref'],
                            cooldown_ignorado_por_escalada=bool(escalada),
                            classe_origem=cls, confidence=r['confianca'],
                            limiar_classe=self.confidencias[cls],
                            frames_exigidos=self.frames_por_classe[cls],
                            inicio_sequencia_s=st['starts'][cls],
                            primeira_confirmacao_s=st['confirmed_at'][cls],
                            espera_cooldown_s=max(0.0, video_s-st['confirmed_at'][cls]),
                            type='confirmacao_local'))
            r.update(contador_desequilibrio=st['counts']['pessoa_desequilibrio'],
                     contador_queda=st['counts']['pessoa_queda'],
                     contador_normal=st['normal'], episodio_ativo=st['active'],
                     gravidade_episodio=st['severity'] or '', episodio=st['episode'],
                     evento_referencia=st['event_ref'] or '',
                     associado_evento_existente=st['associated'])
        for tid, st in list(self.states.items()):
            if tid not in seen:
                self.clear_risk(st)
                st['normal'] = 0
                # Ausência não confirma recuperação. Um ID perdido por muito
                # tempo é descartado; reaparecimento pode produzir outro evento.
                if index - st['last'] > self.fps * 30:
                    self.transitions.append(dict(frame=index, tempo_video_s=video_s,
                        track_id=tid, episodio=st['episode'], transicao='expiracao_id_ausente',
                        gravidade_anterior=st['severity'], gravidade_nova=None,
                        observacao='Expiração técnica após 30 s; não comprova recuperação.'))
                    del self.states[tid]
        return rows

class MotorTemporalContinuo(MotorTemporal):
    """V4 experimental: N evidências reais de queda em até N+T frames.

    Normalidade e desequilíbrio continuam estritamente consecutivos.
    Ausência não conta como normal nem como queda. Não remove caixas.
    Um handoff transfere somente evidência pendente, nunca encerra episódios.
    """

    def __init__(self, *args, tolerancia_queda=1, handoff=True,
                 handoff_iou=0.70, handoff_intervalo_s=0.30, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(tolerancia_queda, int) and 0 <= tolerancia_queda <= 2
        assert 0 < handoff_iou <= 1 and handoff_intervalo_s > 0
        self.tolerancia_queda = tolerancia_queda
        self.handoff = handoff
        self.handoff_iou = handoff_iou
        self.handoff_intervalo_s = handoff_intervalo_s
        self.evidencias_queda = {}
        self.ultima_caixa = {}
        self.ultima_presenca = {}
        self.ultima_classe_aceita = {}
        self.auditoria_continuidade = []
        self._indice_anterior = 0

    def step(self, index, video_s, detections, track=True):
        if not track:
            return super().step(index, video_s, detections, track=False)
        assert index > self._indice_anterior, 'Frames precisam avançar; não processe um frame duas vezes.'
        self._indice_anterior = index
        # Mesma escolha por ID do V3; nenhuma supressão entre classes/IDs.
        chosen = {}
        for d in detections:
            tid = d['track_id']
            if tid is None:
                continue
            key = (d['confianca'] >= self.confidencias[d['classe']], d['confianca'])
            if tid not in chosen or key > chosen[tid][0]:
                chosen[tid] = (key, d)
        atuais = {tid: pair[1] for tid, pair in chosen.items()}
        janela = self.frames_por_classe['pessoa_queda'] + self.tolerancia_queda
        for tid, hist in list(self.evidencias_queda.items()):
            while hist and hist[0][0] < index-janela+1:
                hist.popleft()
            if not hist:
                del self.evidencias_queda[tid]

        # Handoff somente quando a referência está ausente e sem episódio ativo.
        # A correspondência precisa ser única nos dois sentidos. Duas pessoas
        # atuais disputando a mesma referência impedem a transferência.
        pares = {}
        disputas = Counter()
        if self.handoff:
            # Inclui caixas normais/desequilíbrio na verificação de disputa.
            for old, hist in self.evidencias_queda.items():
                if old in atuais or not hist or self.states.get(old, {}).get('active', False):
                    continue
                gap = video_s-self.ultima_presenca.get(old, float('-inf'))
                if not 0 <= gap <= self.handoff_intervalo_s:
                    continue
                for tid, d in atuais.items():
                    if d['confianca'] < self.confidencias[d['classe']]:
                        continue
                    box = tuple(d[k] for k in ('x1','y1','x2','y2'))
                    if self.box_iou(box, self.ultima_caixa[old]) >= self.handoff_iou:
                        disputas[old] += 1
            for tid, d in atuais.items():
                if d['classe'] != 'pessoa_queda' or d['confianca'] < self.confidencias['pessoa_queda']:
                    continue
                if self.states.get(tid, {}).get('active', False):
                    continue
                box = tuple(d[k] for k in ('x1','y1','x2','y2'))
                for old, hist in self.evidencias_queda.items():
                    if old == tid or old in atuais or not hist:
                        continue
                    if self.states.get(old, {}).get('active', False):
                        continue
                    # Uma caixa rejeitada mantém apenas a geometria; não é
                    # prova de recuperação nem evidência positiva de queda.
                    if self.ultima_classe_aceita.get(old) in ('pessoa_normal', 'pessoa_desequilibrio'):
                        continue
                    gap = video_s-self.ultima_presenca.get(old, float('-inf'))
                    if not 0 <= gap <= self.handoff_intervalo_s:
                        continue
                    overlap = self.box_iou(box, self.ultima_caixa[old])
                    if overlap >= self.handoff_iou:
                        pares.setdefault(tid, []).append((old, overlap, gap))
            usados = Counter(old for opts in pares.values() for old, _, _ in opts)
            # Fotografia anterior: não encadeia transferências no mesmo frame.
            transferencias = [(tid, opts[0]) for tid, opts in pares.items()
                              if len(opts) == 1 and usados[opts[0][0]] == 1
                              and disputas[opts[0][0]] == 1]
            for tid, (old, overlap, gap) in transferencias:
                origem = list(self.evidencias_queda.get(old, []))
                destino = list(self.evidencias_queda.get(tid, []))
                unidos = {f: (f, t) for f, t in origem+destino}
                self.evidencias_queda[tid] = deque(unidos[f] for f in sorted(unidos))
                self.evidencias_queda.pop(old, None)
                self.auditoria_continuidade.append(dict(frame=index, tempo_video_s=video_s,
                    transicao='handoff_evidencia_pendente', id_origem=old, track_id=tid,
                    iou=overlap, intervalo_s=gap, evidencias_transferidas=len(origem),
                    frames_origem=[f for f, _ in origem],
                    frames_destino_anteriores=[f for f, _ in destino],
                    referencia_geometrica_aceita=self.ultima_classe_aceita.get(old) is not None))

        for tid, d in atuais.items():
            self.ultima_caixa[tid] = tuple(d[k] for k in ('x1','y1','x2','y2'))
            self.ultima_presenca[tid] = video_s
            self.ultima_classe_aceita[tid] = (d['classe'] if d['confianca'] >= self.confidencias[d['classe']] else None)
            if d['classe'] == 'pessoa_queda' and d['confianca'] >= self.confidencias['pessoa_queda']:
                hist = self.evidencias_queda.setdefault(tid, deque())
                hist.append((index, video_s))
                st = self.states.setdefault(tid, self.new_state())
                # O super.step acrescentará somente a evidência atual. As
                # interrupções toleradas nunca viram evidências positivas.
                st['counts']['pessoa_queda'] = len(hist)-1
                st['starts']['pessoa_queda'] = hist[0][1]
                if len(hist) < self.frames_por_classe['pessoa_queda']:
                    st['confirmed_at']['pessoa_queda'] = None
                st['last'] = index-1
        rows = super().step(index, video_s, detections, track=True)
        for r in rows:
            tid = r['track_id']
            hist = self.evidencias_queda.get(tid, [])
            r['evidencias_queda_janela'] = len(hist)
            r['janela_queda_frames'] = janela
            r['interrupcoes_queda_janela'] = ((index-hist[0][0]+1)-len(hist)) if hist else 0
            if r.get('evento_emitido') and r['classe'] == 'pessoa_queda':
                evento = next(e for e in reversed(self.events) if e['frame'] == index and e['track_id'] == tid)
                evento.update(criterio_temporal='evidencias_reais_em_janela_limitada',
                    janela_max_frames=janela, evidencias_queda=len(hist),
                    tolerancia_frames=self.tolerancia_queda,
                    frames_evidencias=[f for f, _ in hist])
            if r.get('episodio_encerrado'):
                self.evidencias_queda.pop(tid, None)
        # Limita metadados de IDs ausentes; isso não declara recuperação.
        for tid in list(self.ultima_presenca):
            if video_s-self.ultima_presenca[tid] > 30 and tid not in self.states:
                self.ultima_presenca.pop(tid, None)
                self.ultima_caixa.pop(tid, None)
                self.ultima_classe_aceita.pop(tid, None)
                self.evidencias_queda.pop(tid, None)
        return rows
