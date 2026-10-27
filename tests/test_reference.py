import unittest
from motor_temporal_v4 import MotorTemporal, MotorTemporalContinuo, CLASSES_RISCO

def testar_motor():
    conf = {'pessoa_normal': .30, 'pessoa_desequilibrio': .65, 'pessoa_queda': .50}
    frames = {'pessoa_desequilibrio': 15, 'pessoa_queda': 15}
    def novo(escalada=True):
        return MotorTemporal(conf, frames, set(CLASSES_RISCO), cooldown=3, permitir_escalada=escalada)
    def alimentar(m, seq):
        for i, cls in enumerate(seq, 1):
            ds = [] if cls is None else [dict(track_id=1, classe=cls, confianca=.95,
                                             x1=0, y1=0, x2=100, y2=100)]
            m.step(i, (i-1)/15, ds)
        return m
    D, Q, N = 'pessoa_desequilibrio', 'pessoa_queda', 'pessoa_normal'
    m=alimentar(novo(), [D]*15+[Q]*30)
    assert [e['tipo_evento'] for e in m.events] == ['abertura_episodio','elevacao_gravidade']
    assert [e['episodio'] for e in m.events] == [1,1]
    assert m.events[1]['tempo_video_s']-m.events[0]['tempo_video_s'] < 3
    assert len(alimentar(novo(False), [D]*15+[Q]*30).events)==1
    assert len(alimentar(novo(), [D]*8+[Q]*7).events)==0
    m=alimentar(novo(), [Q]*15+[N]*15+[Q]*45)
    assert [e['episodio'] for e in m.events]==[1,2]
    assert any(t['transicao']=='encerramento_recuperacao' for t in m.transitions)
    assert len(alimentar(novo(), [Q]*15+[None]*15+[Q]*45).events)==1
    assert len(alimentar(novo(), [Q]*15+[N]*14+[Q]*45).events)==1
    assert len(alimentar(novo(), [Q]*90).events)==1
    assert len(alimentar(novo(), [Q]*15+[D]*60).events)==1
    m=novo()
    for i in range(1,31):
        m.step(i,(i-1)/15,[dict(track_id=None,classe=Q,confianca=.99)])
    assert not m.events
    m=novo()
    for i in range(1,31):
        m.step(i,(i-1)/15,[dict(track_id=1,classe=Q,confianca=.49)])
    assert not m.events
    print('10 cenários do motor verificados: elevação, legado, contadores, rearme, ausência, recuperação incompleta, repetição, gravidade, sem ID e baixa confiança.')

def testar_v3():
    conf = {'pessoa_normal': .30, 'pessoa_desequilibrio': .65, 'pessoa_queda': .50}
    frames = {'pessoa_desequilibrio': 15, 'pessoa_queda': 15}
    Q, N, D = 'pessoa_queda', 'pessoa_normal', 'pessoa_desequilibrio'
    A=(0,0,100,200); B=(300,0,400,200)
    feitos=[]
    def make(dedup=True):
        return MotorTemporal(conf,frames,{Q,D},cooldown=3,fps=15,deduplicar_quedas=dedup)
    def det(tid,cls=Q,box=A,score=.95):
        return dict(track_id=tid,classe=cls,confianca=score,
                    **dict(zip(('x1','y1','x2','y2'),box)))
    def feed(m, blocks):
        i=0
        for count, ds in blocks:
            for _ in range(count):
                i+=1; m.step(i,(i-1)/15,ds)
        return m
    def assoc(m):return [t for t in m.transitions if t['transicao']=='associacao_duplicata_queda']
    m=feed(make(),[(30,[det(1)]),(45,[det(2)])])
    assert len(m.events)==1 and len(assoc(m))==1
    feitos.append('troca de ID e persistência não duplicam a mesma queda')
    m=feed(make(False),[(30,[det(1)]),(45,[det(2)])])
    assert len(m.events)==2
    feitos.append('modo V2 sem proteção reproduz duplicação')
    m=feed(make(),[(30,[det(1)]),(30,[det(2)]),(15,[det(2,N)]),(45,[det(2)])])
    assert len(m.events)==2 and m.events[-1]['track_id']==2 and m.events[-1]['episodio']==2
    feitos.append('ID associado recupera, rearma e emite nova queda')
    m=feed(make(),[(30,[det(1)]),(30,[]),(45,[det(2)])])
    assert len(m.events)==2 and not assoc(m)
    feitos.append('intervalo maior que limite não associa posição antiga')
    m=feed(make(),[(30,[det(1)]),(60,[det(2,box=B)])])
    assert len(m.events)==2 and not assoc(m)
    feitos.append('pessoas espacialmente separadas não são associadas')
    m=feed(make(),[(15,[det(1,D)]),(45,[det(1)])])
    assert len(m.events)==2 and m.events[1]['tipo_evento']=='elevacao_gravidade'
    feitos.append('elevação de desequilíbrio para queda preservada')
    m=feed(make(),[(15,[det(1)]),(15,[det(1,N)]),(45,[det(1)])])
    assert len(m.events)==2
    feitos.append('rearme normal do mesmo ID preservado')
    m=feed(make(),[(15,[det(1)]),(15,[]),(45,[det(1)])])
    assert len(m.events)==1
    feitos.append('ausência não é recuperação')
    m=feed(make(),[(30,[det(1)]),(45,[det(None)])])
    assert len(m.events)==1 and not assoc(m)
    feitos.append('detecção sem ID não abre evento nem associação')
    m=feed(make(),[(30,[det(1)]),(45,[det(2,score=.49)])])
    assert len(m.events)==1 and not assoc(m)
    feitos.append('confiança abaixo do limite não confirma queda')
    m=feed(make(),[(8,[det(1,D)]),(7,[det(1)])])
    assert not m.events
    feitos.append('contadores de classes não se somam')
    m=make()
    # Duas referências de episódios diferentes na mesma posição: associação ambígua.
    for tid in [1,2]:
        st=m.new_state();st.update(active=True,severity=Q,fall_box=A,fall_s=1.,event_ref=f'cam:{tid}:1')
        m.states[tid]=st
    cand,amb=m.duplicate_fall(3,A,1.1,m.states)
    assert cand is None and amb
    feitos.append('ambiguidade entre ocorrências não suprime por associação')
    m=feed(make(),[(30,[det(1)]),(15,[det(1,N),det(2)]),(45,[det(2)])])
    assert len(m.events)==2 and not assoc(m)
    feitos.append('normalidade atual da referência impede associação à caixa antiga')
    # As caixas entram em ordem inversa; a decisão deve ser a mesma.
    seq=[(30,[det(1)]),(45,[det(1),det(2)])]
    ma=feed(make(),seq); mb=feed(make(),[(c,list(reversed(ds))) for c,ds in seq])
    assert [(e['frame'],e['track_id']) for e in ma.events]==[(e['frame'],e['track_id']) for e in mb.events]
    assert len(assoc(ma))==len(assoc(mb))==1
    feitos.append('ordem das caixas não altera a associação neste cenário')
    print(f'{len(feitos)} cenários V3 verificados:')
    for item in feitos:print('•',item)
    return feitos

def testar_continuidade():
    conf = {'pessoa_normal': .30, 'pessoa_desequilibrio': .65, 'pessoa_queda': .50}
    frames = {'pessoa_desequilibrio': 15, 'pessoa_queda': 10}
    Q, N, D = 'pessoa_queda', 'pessoa_normal', 'pessoa_desequilibrio'
    A, B = (0,0,100,200), (400,0,500,200)
    feitos = []
    def novo(**opts):
        return MotorTemporalContinuo(conf, frames, {Q,D}, fps=15, cooldown=0,
            deduplicar_quedas=True, **opts)
    def d(tid=1, cls=Q, box=A, score=.95):
        return dict(track_id=tid,classe=cls,confianca=score,
            **dict(zip(('x1','y1','x2','y2'),box)))
    def feed(m, blocks):
        i=0
        for n, ds in blocks:
            for _ in range(n):
                i+=1; m.step(i,(i-1)/15,ds)
        return m
    def check(ok, label):
        assert ok, label
        feitos.append(label)
    check(not feed(novo(),[(9,[d()])]).events, 'nove evidências não confirmam dez')
    m=feed(novo(),[(5,[d()]),(1,[d(cls=N)]),(5,[d()])])
    check(len(m.events)==1 and m.events[0]['frame']==11, 'um frame normal interrompido é tolerado sem contar como queda')
    check(not feed(novo(),[(5,[d()]),(2,[d(cls=N)]),(5,[d()])]).events,
          'duas interrupções excedem a janela padrão')
    check(len(feed(novo(),[(5,[d()]),(1,[]),(5,[d()])]).events)==1,
          'um frame ausente é tolerado, sem fabricar detecção')
    check(not feed(novo(),[(9,[d()]),(1,[])]).events,
          'não confirma no frame ausente')
    check(len(feed(novo(),[(5,[d()]),(1,[d(score=.49)]),(5,[d()])]).events)==1,
          'baixa confiança pode ser interrupção, nunca evidência positiva')
    m=feed(novo(),[(5,[d(1)]),(5,[d(2)])])
    check(len(m.events)==1 and m.events[0]['track_id']==2 and len(m.auditoria_continuidade)==1,
          'troca única de ID transfere evidência pendente')
    check(not feed(novo(),[(5,[d(1)]),(5,[d(2,box=B)])]).events,
          'distância espacial impede transferência')
    check(not feed(novo(),[(5,[d(1)]),(10,[]),(5,[d(2)])]).events,
          'intervalo longo impede transferência')
    m=feed(novo(),[(5,[d(1),d(2)]),(5,[d(3)])])
    check(not m.events and not m.auditoria_continuidade,
          'duas origens compatíveis impedem handoff ambíguo')
    m=feed(novo(),[(5,[d(1)]),(5,[d(2),d(3,cls=N)])])
    check(not m.events and not m.auditoria_continuidade,
          'duas pessoas atuais, inclusive normal, impedem disputar a mesma origem')
    m=feed(novo(),[(10,[d(1)]),(25,[d(2)])])
    check(len(m.events)==1 and any(t['transicao']=='associacao_duplicata_queda' for t in m.transitions)
          and not m.auditoria_continuidade, 'episódio confirmado usa proteção V3 e não transfere contagem ativa')
    m=feed(novo(),[(15,[d(cls=D)]),(10,[d()])])
    check([e['tipo_evento'] for e in m.events]==['abertura_episodio','elevacao_gravidade'],
          'elevação de desequilíbrio para queda preservada')
    m=feed(novo(),[(10,[d()]),(15,[d(cls=N)]),(10,[d()])])
    check([e['episodio'] for e in m.events]==[1,2], 'quinze normais encerram e permitem nova queda')
    check(len(feed(novo(),[(10,[d()]),(14,[d(cls=N)]),(10,[d()])]).events)==1,
          'quatorze normais não rearmam')
    check(len(feed(novo(),[(10,[d()]),(15,[]),(10,[d()])]).events)==1,
          'ausência não rearma episódio ativo')
    check(not feed(novo(),[(8,[d(cls=D)]),(7,[d()])]).events,
          'desequilíbrio não é somado à queda')
    check(not feed(novo(),[(30,[d(tid=None)])]).events, 'sem ID não abre episódio')
    check(not feed(novo(),[(30,[d(score=.49)])]).events, 'rejeitadas não confirmam episódio')
    check(not feed(novo(),[(5,[d(),d()])]).events, 'mesmo ID não acumula duas vezes por frame')
    m=feed(novo(),[(10,[d(1),d(2,box=B)])])
    check(len(m.events)==2 and all(e.get('evidencias_queda')==10 for e in m.events),
          'duas pessoas separadas mantêm evidências e metadados independentes')
    m=novo();m.step(1,0,[d()])
    try:
        m.step(1,0,[d()])
        raise AssertionError('frame repetido aceito')
    except AssertionError as ex:
        assert 'Frames precisam' in str(ex)
    feitos.append('reprocessamento do mesmo frame é rejeitado')
    # Limite zero mantém confirmação estrita em uma sequência interrompida.
    check(not feed(novo(tolerancia_queda=0,handoff=False),[(5,[d()]),(1,[]),(5,[d()])]).events,
          'tolerância zero restaura confirmação consecutiva')
    print(f'{len(feitos)} cenários V4 verificados.')
    return feitos

class ReferenceTest(unittest.TestCase):
    def test_47_original_scenarios(self):
        testar_motor()
        testar_v3()
        testar_continuidade()
