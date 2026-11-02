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
fim, lacunas de amostragem, interrupções de continuidade e eventos são independentes
do contador do resumo. Não existe espera ou salto de inferência para produzir logs.

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

Resultado local em 02/11/2026: **79 testes passaram, sem skips**, em Python 3.12.14
no Windows, com NumPy 2.1.3 e OpenCV 4.11.0.86 instalados apenas em `.test-tmp/deps`.
O log está em `.test-tmp/homologation-tests.log` (artefato local ignorado pelo Git
e pelo build). Sintaxe Python e `docker compose config --quiet` foram validados,
tanto para o Compose base quanto com o override GPU, sem iniciar containers.
A CI foi ajustada para instalar as duas dependências de imagem antes dos testes;
esse workflow não foi executado remotamente.

Pendente no ambiente real: construir/validar a imagem Linux, permissões UID/GID no
volume da VPS, câmera e pesos reais, FPS/latência sob carga, retenção/espaço e
entrega broker→n8n→API→app. Os testes locais não demonstram essas propriedades.
