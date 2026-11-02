# Homologação do stream

Na Stack, usar uma imagem construída com esta versão e definir `LOG_MODE=homologation`.
Manter a fonte privada na variável `SOURCE`, `SOURCE_MODE=stream` e os demais parâmetros
de motor/publicação já aprovados para o ensaio. Este modo não habilita publicação nem
altera TEST_MODE, limiares, tracking, confirmação, cooldown, heartbeat ou transporte.
Não foi realizado build, push, deploy ou acesso à VPS nesta implementação.

## Variáveis

| Variável | Padrão | Efeito |
|---|---|---|
| `LOG_MODE` | `production` no código/Compose | Aceita também `development` e `homologation`; o exemplo de ambiente mantém `development` |
| `HOMOLOGATION_LOG_EVERY_N_FRAMES` | `100` | Um resumo a cada N frames **processados**, sem pular chamadas de inferência/motor |
| `HOMOLOGATION_DIAGNOSTICS_ENABLED` | `false` | Auditoria detalhada somente em arquivo e somente em homologation |
| `HOMOLOGATION_DIAGNOSTICS_MAX_FRAMES` | `600` | Máximo de frames auditados por run; inteiro positivo |
| `CAPTURES_ENABLED` | vazio = automático | Ativa em homologation; desativa nos outros modos. `true`/`false` sobrescrevem |
| `CAPTURES_INTERVAL_S` | `1` | Intervalo mínimo por câmera entre amostras sem evento; eventos ignoram o intervalo |
| `CAPTURES_MAX_COUNT` | `1000` | Máximo de JPEGs por execução |
| `CAPTURES_MAX_BYTES` | `524288000` (500 MiB) | Orçamento por execução: JPEGs mais bytes do índice |

Limites e intervalos devem ser positivos; intervalos devem ser finitos. Uma câmera
por processo/volume, como no worker existente. O relógio de amostragem é monotônico
em stream e nominal em vídeo; não depende da frequência de logs.

## Logs e saúde

Em homologation, não há `deteccoes.jsonl`, `v4.request` nem `v4.response`.
`worker.homologation_summary` concentra a saída periódica; não duplica
`worker.metrics` nem avisos periódicos de FPS abaixo da referência. O health.json
continua atualizado por `METRICS_INTERVAL_S`, incluindo backlog, idades e desempenho.
Mudança para estado degradado gera aviso imediato na próxima verificação de saúde.
Heartbeat local mantém a frequência original; captura, reconexões, falhas, início,
fim, interrupções efetivas de continuidade e eventos são independentes
do contador do resumo. Não existe espera ou salto de inferência para produzir logs.

Em homologation, `v4.sampling_gap` não é emitido individualmente. O resumo acumula
`sampling_gap_count`, `sampling_gap_max_s` (valor sem arredondamento) e
`continuity_interruptions` desde o último resumo, zerando-os após cada N frames.
`v4.continuity_interrupted` permanece imediato quando uma lacuna realmente limpa
contagens de risco/recuperação ou evidências de queda. Lacuna com estado já vazio
não conta como interrupção efetiva. Episódios ativos continuam preservados.
O intervalo final incompleto não gera resumo adicional; lacunas constam também da
auditoria enquanto ela estiver ativa. Os demais modos mantêm o aviso de sampling gap.

O resumo contém contagens de caixas por classe no frame atual e acumuladas desde o
último resumo, IDs presentes, frame/segmento, FPS desde o início, descartes e
p50/p95/máximo de inferência, motor, pipeline e idade dos frames. Os quantis usam a
janela limitada `METRICS_WINDOW` (1024 por padrão). Backlog e idade do evento mais
antigo refletem a última atualização de saúde, não uma consulta extra por frame.
As contagens de detecção representam as saídas YOLO validadas, antes da aceitação
por confiança do motor; as imagens amostradas usam somente caixas aceitas.

Exemplos ilustrativos abreviados (não medições reais):

```json
{"event":"worker.homologation_summary","frame":100,"segment":1,"frames":100,"detections_by_class":{"pessoa_queda":1},"interval_detections_by_class":{"pessoa_normal":80,"pessoa_queda":20},"track_ids":[7],"fps_effective":14.8,"frame_age_ms":{"p50":8,"p95":15,"max":20},"inference_ms":{"p50":42,"p95":50,"max":55}}
{"event":"risk.event","message":"🔴 CRITICA evento=abc123 classe=pessoa_queda track=7 frame=105 observado_em=2026-11-02T10:00:00.000Z","severity":"CRITICA","event_ref":"abc123","classe":"pessoa_queda","track_id":7,"frame":105,"observed_at":"2026-11-02T10:00:00.000Z","engine":"v4"}
{"event":"risk.event","message":"🟡 ATENCAO evento=def456 classe=pessoa_desequilibrio track=8 frame=115 observado_em=2026-11-02T10:00:01.000Z","severity":"ATENCAO","event_ref":"def456","classe":"pessoa_desequilibrio","track_id":8,"frame":115,"observed_at":"2026-11-02T10:00:01.000Z","engine":"v4"}
```

Campos existentes como `event_kind`, `evidence_count`, `run_ref` e a severidade
estruturada permanecem. Não há URL privada, payload, credenciais ou Base64 nos logs.

## Capturas e coleta

Container: `/dados/runs/<run_id>/capturas/`. Host:
`${HOST_DATA_DIR}/runs/<run_id>/capturas/`; padrão do Compose:
`/opt/poolsafe/dados/ensaio-modelo-002/runs/<run_id>/capturas/`.
O mount persistente existente é reutilizado, sem um volume adicional.
O arquivo `${HOST_DATA_DIR}/latest_run` identifica a execução concluída mais recente.
Durante execução, o run aparece no log de início como referência e pode ser localizado
nos diretórios `runs` do volume.

Arquivos `evento_s<segmento>_f<frame>_<event_ref>.jpg` correspondem a cada evento
ATENCAO/CRITICA confirmado, inclusive vários eventos no mesmo frame.
`amostra_s<segmento>_f<frame>_sem_evento.jpg` é a amostra de detecções aceitas sem
evento confirmado. Quadros sem caixas aceitas não geram amostras. Eventos também
reiniciam o intervalo mínimo de amostragem para evitar repetição imediata.

`capturas/indice.jsonl` tem uma linha por JPEG: `path` relativo ao diretório do run,
`run`, `segment`, `processed_frame`, `captured_frame`, `observed_at` UTC,
`video_time_s` nominal (null em stream), `width`, `height`, `kind`, `boxes` com
`bbox_xyxy`, `classe`, `confianca` e `track_id`, além de `event_ref` e `event` quando
houver. A referência coincide com `risk.event` e `eventos.jsonl`. Coordenadas são
arredondadas e limitadas à resolução original salva; não há redimensionamento.

A anotação usa caixas da inferência existente sobre uma cópia do frame. A imagem
de inferência e a evidência sem anotações do pipeline permanecem intactas. JPEG e
índice são gravados em uma thread separada; a fila tem 32 frames e prioriza eventos.
Eventos substituem uma amostra pendente quando necessário, com aviso
`sample_evicted_for_event`. Se a fila lotar somente com eventos, ou se uma nova
amostra encontrar a fila cheia, a solicitação é descartada com `capture.warning` e código
`queue_full_event` ou `queue_full_sample`; não bloqueia inferência/publicação.
Falhas de codificação/permissão/disco também são avisadas pelo tipo da exceção,
sem sua mensagem potencialmente sensível. Cada código avisa uma vez por run.
Capturas são auxiliares e de melhor esforço; `eventos.jsonl` e outbox continuam
independentes. Falhas entre JPEG e índice podem deixar um JPEG órfão, contabilizado
no limite de espaço.

Ao alcançar quantidade/orçamento, `storage_limit_reached` suspende capturas pelo
restante do run, inclusive de eventos; o worker continua. Limites são **por run**:
retenção global de execuções anteriores continua sendo responsabilidade operacional.
Não apagar outbox/SQLite/WAL ao coletar ou remover imagens.

O container mantém `USER 10001:10001`. Antes da execução, o diretório de dados do
host precisa permitir escrita para esse UID/GID, como exigido pelo volume existente.
Não há chmod global nem execução como root. Para coletar, copiar pelo canal privado
operacional o diretório completo `runs/<run_id>` (ou capturas mais eventos/resumo).
Exemplo a executar pelo operador no host, substituindo o run e o destino:

```sh
tar -C /opt/poolsafe/dados/ensaio-modelo-002/runs -czf /destino/ensaio.tar.gz <run_id>
```

Imagens contêm pessoas: restringir acesso e aplicar a retenção do ensaio.
A inspeção é humana; não se calcula precisão/recall automaticamente sem anotações.

## Diagnóstico detalhado opcional

Para o próximo ensaio, definir no ambiente/Compose existente:

```dotenv
LOG_MODE=homologation
HOMOLOGATION_DIAGNOSTICS_ENABLED=true
HOMOLOGATION_DIAGNOSTICS_MAX_FRAMES=600
```

Preservar fonte, pesos, política e parâmetros de publicação do ensaio. Nenhuma
dessas variáveis habilita publicação. O override GPU herda as duas variáveis do
Compose base. Coletar `/dados/runs/<run_id>/diagnostico_frames.jsonl`,
`resumo.json` e `servico.log` no volume já existente; o comando tar acima inclui
esses arquivos. Para desabilitar no próximo run, usar `false`.

Cada linha corresponde a um frame **processado**, inclusive sem detecções. Frames
descartados pelo slot/idade antes da inferência não são auditados. O limite conta
frames por execução, não por segmento e não por caixa. Ao gravar o último frame,
`diagnostics.warning` com `code=frame_limit_reached` avisa uma única vez e suspende
apenas a auditoria. O motor, eventos e capturas continuam. Registros individuais
não passam pelo logger nem pelo console. Não são produzidas imagens novas por
esta funcionalidade: o mecanismo de JPEGs mantém seus critérios atuais.

Falha ao criar diretório, serializar, abrir, escrever ou fechar o arquivo suspende
a auditoria, com aviso único `write_failed` e somente o tipo da exceção. Isso não
interrompe detecção/publicação. Uma falha de disco pode deixar a última linha
incompleta; preservar o arquivo original e ignorar essa linha na análise. O bloco
`diagnostico` de `resumo.json` informa habilitação efetiva, tentativas, linhas
gravadas, suspensão, avisos, versão e último status da observação pré-tracker.

Campos principais: run/segmento, frames processado e capturado, `observado_em`
UTC e `referencia_temporal` (monotônico de captura no stream; nominal do vídeo,
com primeiro frame em 0 s). Esses dois relógios não devem ser subtraídos entre si.
`observacoes` são as linhas reais do motor, com limiar, aceitação, uso temporal,
motivo, contagens, evidências na janela, confirmação, recuperação e episódio.
`estado_antes`/`estado_depois` fotografam os estados e os frames/tempos de evidência
de queda, incluindo IDs ausentes neste frame. `ids_ausentes` significa ausência
de ID na entrada do motor, sem atribuir causa a oclusão, detector ou tracker.
`continuidade` traz handoffs e lacunas, inclusive `limpeza_efetiva` e os IDs com
evidências limpas; `transicoes` e `decisoes` preservam a resposta do motor.
Comparar antes/depois permite ver resets por ausência ou rejeição e expiração de
evidências da janela sem confundi-los com recuperação confirmada. O V4 pode
reter evidências de queda na janela mesmo quando zera contagens consecutivas.

Exemplos **abreviados e ilustrativos**, não reconstruções do ensaio anterior:

```json
{"frame_processado":111,"sem_deteccao":true,"observacoes":[],"decisoes":[]}
{"frame_processado":112,"deteccoes_sem_id":1,"observacoes":[{"track_id":null,"classe":"pessoa_queda","confianca":0.528,"limiar_classe":0.5,"aceita":true,"usada_temporal":false,"confirmacao_temporal":false,"motivo":"sem_id"}],"decisoes":[]}
{"frame_processado":113,"observacoes":[{"track_id":7,"confianca":0.49,"aceita":false,"motivo":"abaixo_limiar"}]}
{"frame_processado":114,"observacoes":[{"track_id":7,"aceita":true,"confirmacao_temporal":false,"motivo":"aguardando_persistencia","evidencias_queda_janela":1}]}
```

Assim, uma queda com 0,528 pode passar pelo limiar 0,50 e continuar sem evento
por `sem_id`; isso difere de rejeição por confiança e de persistência incompleta.
O arquivo anterior de capturas é amostrado e não permite recuperar retroativamente
os passos que não foram registrados.

### Etapas do detector/tracker e limites técnicos

O requirements fixa `ultralytics==8.4.173`. Foram inspecionados os fontes do wheel
exato obtido do PyPI e os fontes oficiais de
[tracking](https://github.com/ultralytics/ultralytics/blob/v8.4.173/ultralytics/trackers/track.py),
[predictor](https://github.com/ultralytics/ultralytics/blob/v8.4.173/ultralytics/engine/predictor.py) e
[detecção](https://github.com/ultralytics/ultralytics/blob/v8.4.173/ultralytics/models/yolo/detect/predict.py).
No Python local de validação, Ultralytics não está instalado; não foi realizada
inferência com pesos/câmera reais. A versão instalada no worker é obtida por
metadata em runtime e gravada no diagnóstico.

1. `etapas.detector_pos_nms`: cópia das caixas de `predictor.results` no callback
   observador `on_predict_postprocess_end`, registrado no modelo novo antes da
   primeira chamada `track`. O pós-processamento já aplicou confiança de
   inferência (`min(CONF_NORMAL, CONF_DESEQUILIBRIO, CONF_QUEDA)`), NMS/IoU,
   seleção de classes e `max_det` da biblioteca, com coordenadas escaladas à
   imagem original. Os parâmetros efetivos disponíveis ficam em
   `parametros_posprocessamento`; não há acesso às caixas abaixo desse limiar
   nem às descartadas por NMS. No YOLO11 de detecção, esse ponto é pós-NMS.
2. `etapas.apos_tracker`: resultado retornado pela mesma chamada `model.track`,
   com IDs ou `null`, exatamente como recebido pelo motor. Em 8.4.173, se o
   tracker retorna tracks, a biblioteca seleciona as caixas pelos índices dos
   tracks e atualiza os resultados; se não retorna tracks, mantém as caixas
   originais sem ID. Portanto, uma caixa pré-tracker ausente na saída não tem
   automaticamente um motivo individual de descarte conhecido.

Há uma única chamada `track` por frame. Não há `predict` adicional, alteração de
ByteTrack ou reutilização de uma segunda inferência. Os dados são convertidos a
listas e copiados antes da mutação; nenhum tensor/Results é retido ou modificado.
O observador é acrescentado sem substituir, envolver ou reordenar os callbacks
de tracking. O registro normal da biblioteca permanece responsável pelo tracker,
inclusive ao recriar o predictor em reconexões. O hook torna-se inativo ao
encerrar a coleta.

Em versão diferente de 8.4.173, tracker já registrado antes da instalação, API
incompatível ou falha do callback, o diagnóstico registra status
`indisponivel_*` (ou `callback_nao_executado`) e `detector_pos_nms=null`, em vez
de alegar uma observação pré-tracker. `[]` significa etapa observada sem caixas.
A saída pós-tracker e o motor continuam auditáveis. O hook atende o contrato
atual de uma imagem por chamada; batches maiores são marcados indisponíveis.
Não é possível deduzir por essas etapas o motivo interno de cada associação ou
rejeição do ByteTrack. O JSONL é síncrono, com abertura/fechamento por frame para
coleta durante o ensaio; cópias CPU e I/O acrescentam latência e podem afetar a
amostragem de um stream sob carga. As decisões para a mesma sequência de entradas
são preservadas; isso não garante a mesma sequência capturada em ensaios distintos.

## Validação

Local: `PYTHONUTF8=1 python -m unittest discover -s tests -v` (em PowerShell,
`$env:PYTHONUTF8='1'` antes do comando). UTF-8 também evita dependência do encoding
regional na verificação de proveniência existente. Testes JPEG requerem OpenCV e
NumPy do requirements; sem eles, esses testes são explicitamente ignorados.

A suíte verifica periodicidade, eventos fora do intervalo, destaques, saúde,
amostragem, coordenadas/dimensões e JPEG real, isolamento do frame/evidência,
limites e falhas de codificação/permissão. O worker com fixtures é comparado entre
os três modos, incluindo falha de gravação: mesmas decisões, 25 inferências para
25 frames e mesmos envelopes/evidências na outbox.

Resultado local atualizado em 02/11/2026: **91 testes passaram, sem skips**, em Python 3.12.14
no Windows, com NumPy 2.1.3 e OpenCV 4.11.0.86 instalados apenas em `.test-tmp/deps`.
O log está em `.test-tmp/diagnostics-tests.log` (artefato local ignorado pelo Git
e pelo build). Sintaxe Python e `docker compose config --quiet` foram validados,
tanto para o Compose base quanto com o override GPU, sem iniciar containers.
A CI foi ajustada para instalar as duas dependências de imagem antes dos testes;
esse workflow não foi executado remotamente.

Os novos testes cobrem frames vazios/sem ID, baixa confiança versus confirmação
incompleta, estados antes/depois da ausência, limite, falhas de escrita/callback,
versão não validada e tracker já registrado, cópia anterior à mutação, caixas
pós-tracker sem ID e paridade temporal ligada/desligada. O worker com fixtures
mantém 25 chamadas `track` para 25 frames, decisões e envelopes/evidências iguais,
inclusive quando falha a escrita do diagnóstico. O teste de stream verifica 100
chamadas para 100 frames, 99 gaps agregados e um único aviso imediato para uma
limpeza efetiva. Não executa inferência neural ou ByteTrack real.

Para reproduzir neste Windows com o Python 3.12 disponível, apontar `PYTHONPATH`
para `.test-tmp/deps` e `TEMP`/`TMP` para `.test-tmp` (diretório gravável do
workspace), além de `PYTHONUTF8=1`, e executar `python -m unittest discover -s tests -v`.

Pendente no ambiente real: construir/validar a imagem Linux, permissões UID/GID no
volume da VPS, câmera e pesos reais, FPS/latência sob carga, retenção/espaço e
entrega broker→n8n→API→app. Os testes locais não demonstram essas propriedades.
