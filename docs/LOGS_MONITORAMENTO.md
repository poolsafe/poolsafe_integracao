# Logs e monitoramento

## Implementado

Logs operacionais são JSON em stdout e nos arquivos rotativos existentes. Campos principais: `timestamp` UTC, `level`, `service`, `logger`, `event` e mensagem sanitizada. Eventos estruturados acrescentam referências de execução/câmera, frame, segmento, duração e contagens conforme contexto. Referências de UUID são hashes curtos para correlação; não são prova de anonimização.

URLs completas são removidas; chaves sensíveis, base64, evidência e tokens são redigidos. UUIDs completos em mensagens são substituídos por referências. Não há dump de payload AMQP, corpo HTTP, frame ou tensor nos logs operacionais. Exceções internas V4 registram código/tipo; não são convertidas em normalidade. Stack, quando solicitada ao logger, é sanitizada e não inclui variáveis locais.

Em produção, logs de sucesso request/response V4 são amostrados por `V4_LOG_EVERY_N_FRAMES=150`, e respostas com decisões são registradas mesmo fora da amostragem. Em desenvolvimento, cada frame possui auditoria com caixas, limiares, transições e handoff. Usar desenvolvimento por períodos curtos, com retenção e acesso restritos.

Em `LOG_MODE=homologation`, um único `worker.homologation_summary` a cada
`HOMOLOGATION_LOG_EVERY_N_FRAMES=100` substitui os logs periódicos request/response
e metrics. Eventos confirmados mantêm `risk.event` imediato, com mensagem
`🔴 CRITICA` ou `🟡 ATENCAO`. Saúde e heartbeat mantêm suas frequências.
Consulte [variáveis, capturas persistentes e coleta](HOMOLOGACAO_STREAM.md).

## Catálogo de eventos

| Evento | Campos principais | Interpretação |
|---|---|---|
| `v4.request` | frame, segmento, número de caixas, run_ref | Entrada resumida da chamada local |
| `v4.response` | engine_ms, rows, emitted, transitions, handoffs | Resposta validada; não comprova entrega |
| `v4.failure` | error_code, error_type, frame, segmento | Falha de entrada/execução/resposta; worker encerra com outbox preservada |
| `v4.handoff` | frame, transferências | Continuidade pendente aplicada pelo motor |
| `v4.sampling_gap` | gap_s, frame | Intervalo incomum entre observações live; não altera regras |
| `risk.event` | event_ref, frame, severidade, event_kind, evidence_count | Emissão local já interpretada |
| `amqp.publish_confirmed` | event_ref, risk, publish_ms | Broker confirmou; não significa API/app |
| `amqp.publish_failure` | error_type, pending_preserved | Falha de publicação, sem payload |
| `worker.metrics` | FPS, idades, p50/p95/máximo, backlog, descartes | Resumo operacional em janela limitada |
| `worker.sampling_below_reference` | FPS efetivo e FPS de referência | Processamento abaixo da referência configurada |
| `worker.degraded` | idade da captura e inferência | Processo não demonstrou progresso recente |
| `service.message` | mensagem sanitizada | Compatibilidade com logs de ciclo de vida/transporte existentes |

Exemplo ilustrativo, não medição da VPS:

```json
{"timestamp":"2026-10-27T20:00:00.000Z","level":"INFO","service":"poolsafe-worker","event":"v4.response","run_ref":"exemplo","frame":150,"engine_ms":0.4,"rows":1,"emitted":1,"transitions":1,"handoffs":0}
```

`inference_ms` mede a chamada de tracking/inferência com sincronização CUDA quando aplicável; `v4_ms` mede a chamada local do motor e sua conferência de resposta; `pipeline_ms` mede o processamento local do frame, incluindo regra, evidência e enqueue, excluindo publicação assíncrona; `frame_age_ms` é idade desde decodificação no host. Nenhuma dessas medidas isoladas equivale à latência da câmera até o app ou mede buffers anteriores à decodificação.

Quantis usam até `METRICS_WINDOW=1024` observações, não toda a população nem histograma Prometheus. `worker.metrics` é emitido a cada aproximadamente 15 s quando o loop progride. Se inferência travar, a atualização da saúde deixa de progredir; um observador externo deve detectar isso. O heartbeat da câmera permanece responsabilidade da captura e pode continuar nesse cenário.

## Agregação recomendada, sem implantar serviços extras

Fase inicial: `docker compose logs`, Portainer, rotação Docker 10 MiB × três arquivos e logs do serviço já limitados. Evitar encaminhar simultaneamente stdout e a mesma cópia local para não duplicar contagens. O pipeline não depende de Loki para funcionar.

Evolução: Grafana Alloy coleta stdout Docker e encaminha a Loki; Grafana consulta e alerta. Coletor e armazenamento são serviços operacionais separados, não parte do worker. Preferir Loki remoto quando a VPS estiver pressionada; revisar política de privacidade e localização antes de enviar dados. Fixar versões de Alloy/Loki e limitar memória/disco e retenção antes de habilitar.

Se a coleta usar socket Docker, tratar esse acesso como privilégio administrativo; montagem read-only não fornece por si autorização de API somente-leitura. Usar proxy restritivo ou coleta de arquivos conforme a política da infraestrutura. Não montar socket Docker no worker de inferência.

Labels sugeridos: serviço, ambiente e versão de release. `run_ref`, `event_ref`, frame e trackId permanecem conteúdo do log, sem labels de alta cardinalidade. Quando houver poucas câmeras, sua referência pode ser usada como dimensão controlada. Usar Prometheus/cAdvisor ou equivalente para CPU, memória, rede e OOM, e o exportador do broker para fila/consumidores; esses componentes não foram instalados nesta entrega.

Consultas LogQL candidatas:

```logql
{service="poolsafe-worker"} | json | event="v4.failure"
{service="poolsafe-worker"} | json | event="worker.metrics"
{service="poolsafe-worker"} | json | event="amqp.publish_failure"
```

A label `service` deve ser configurada no coletor; existir no JSON não a transforma automaticamente em label Loki.

## Alarmes e runbook

| Condição | Ação |
|---|---|
| V4 falhou/modelo incompatível | Suspender ativação; conferir hash, classes, entrada e versão; não reduzir limiar como reparo |
| Captura recente e inferência antiga | Verificar processo, CPU/GPU, memória e etapa bloqueada |
| Saúde ausente/antiga | Investigar worker parado, trava, disco cheio ou permissões |
| FPS abaixo da referência/p95 crescente | Suspender expansão de câmeras; revisar recursos e benchmark |
| Outbox crescendo ou idade alta | Verificar broker/rotas/n8n; respeitar validade dos alertas |
| Reject-publish no broker | Verificar limite de 128 MiB das filas e consumidores; não apagar mensagens sem diagnóstico |
| Expiração/queda de heartbeat | Conferir instante original e captura; não sintetizar online |
| OOM/disco cheio | Rever quotas, evidência, retenção e concorrência; preservar outbox |

Limiares de alerta devem ser definidos no ensaio da VPS. Não inventar SLO de desempenho geral. Arquivos por execução (`resumo.json`, JSONL e diretórios runs) ainda exigem retenção operacional global; rotação de cada arquivo não limita o número de execuções. Não remover dados pendentes nem SQLite/WAL por rotina genérica de limpeza. Seguir a política acadêmica de retenção/encerramento da fonte da verdade.

Referência oficial da coleta: https://grafana.com/docs/alloy/latest/monitor/monitor-docker-containers/
