# Refatoração da integração para o motor V4

Referência: 27/10/2026. Fontes efetivamente usadas: ZIP `poolsafe_integracao-main(2).zip`, script `poolsafe_validacao_episodios_v4_continuidade.py` e fonte da verdade de 21/10/2026. Não existe API HTTP V4 nos anexos: sua API é a chamada Python local `MotorTemporalContinuo.step(index, video_s, detections, track=True)`.

## Resultado e fronteiras

O caminho ativo de risco usa exclusivamente V4. As classes `MotorTemporal` e `MotorTemporalContinuo` foram extraídas por AST, preservando suas definições. Nenhuma célula de instalação, upload ou download Colab é executada. O original foi preservado como texto em `reference/v4_colab.py.txt`, acompanhado de hash de proveniência. O teste de proveniência compara estruturalmente as duas classes.

O pedido de transferir validações para V4 é aplicado ao que o motor realmente sabe validar. Configuração, TLS, credenciais AMQP, capacidade da outbox, assinatura de imagem e elegibilidade de sessão continuam nas camadas próprias. Removê-las ou fingir que V4 as executa reduziria a integridade do sistema. Sessão, pausa, destinatários e FCM continuam na API/n8n. Não foram criados endpoints nem alterados app, banco ou workflows.

## Estrutura entregue

| Arquivo | Função |
|---|---|
| `motor_temporal_v4.py` | Classes originais da lógica temporal V3/V4; V3 é superclasse da V4, não rota alternativa de produção |
| `engine_adapter.py` | Fronteira tipada de entrada/saída, invocação local, erros e drenagem de resultados |
| `main.py` | Captura, uma inferência, chamada V4, montagem do envelope atual e enqueue |
| `regras.py` | Reexportação da validação de compatibilidade do modelo; sem motor temporal concorrente |
| `runtime_support.py` | Threads CPU/OpenCV e sincronização de GPU quando explicitamente habilitada |
| `observabilidade.py` | Logs JSON sanitizados, métricas em janela limitada e saúde |
| `healthcheck.py` | Verificação local da saúde, sem porta adicional |
| `transporte.py` | Outbox e AMQP existentes, com instrumentação de confirmação/falha |
| `captura.py` | Captura/reconexão existentes, preservadas |
| `config.py` | Ambiente, recursos e configuração da política enviada à V4 |
| `tests/fixtures/legacy_regras.py` | Motor anterior apenas como fixture histórica, fora da imagem Docker |

## Inventário das validações e destino

| Validação anterior | Destino nesta entrega |
|---|---|
| Confiança global como aceitação de risco | Limiar por classe no núcleo V4; filtro de inferência usa o mínimo desses limiares |
| Oito frames por severidade | Persistência própria do V4: 15 desequilíbrios e dez quedas em até onze frames |
| Seleção de observação e interrupções no worker | Todas as caixas seguem para V4; ele escolhe por ID e interpreta ausência/rejeição |
| Cooldown por severidade com supressão definitiva | Política V4: abertura pendente reavaliada, cooldown por câmera e elevação livre |
| Rearme e flags `active_*` do worker | Recuperação por normalidade aceita no V4 |
| Continuidade e duplicação sem geometria | Handoff pendente e associação de queda do V4 |
| Expiração após dois segundos | Expiração técnica original V4, calculada por índice e FPS do contexto |
| Compatibilidade de classes dos pesos | Fachada V4 exige exatamente três classes distintas e IDs 0/1/2, respeitando qualquer ordem dos nomes |
| Estrutura e dimensões das saídas YOLO | Fachada V4 rejeita dimensões incompatíveis, ID fracionário, valores não finitos e caixas inválidas |
| Estrutura da resposta V4 | Fachada verifica linhas, flags e correspondência de eventos, sem recalcular decisões temporais |
| Configuração de arquivos, publicação e credenciais | `Config.validate`, pois não são regras de risco |
| AMQP, SQLite, evidência e legado de mensagens | Transporte/infraestrutura existentes; não pertencem ao motor de visão |
| Autorização, sessão, pausa e destinatários | API/n8n existentes |

O motor legado foi retirado do caminho ativo. Não há fallback para oito frames nem para modelo de duas classes. `ENGINE_VERSION=legacy` é rejeitado. Os testes históricos continuam identificados como fixture, não evidência de paridade V4.

## Contrato de entrada e de saída

Entrada da fachada:

```python
decisions = motor.update_detections(
    [{"track_id": 7, "classe": "pessoa_queda", "confianca": 0.82,
      "bbox_xyxy": [12.5, 20.0, 100.0, 180.0]}],
    frame_number=120,
    now=7.933333,
)
```

A fachada converte `bbox_xyxy` para `x1/y1/x2/y2` e passa todas as caixas ao `step`. Inclui caixas retornadas sem ID e rejeitadas pelo limiar; nenhuma evidência abaixo do limiar vira queda. Não é possível recuperar caixas que detector/NMS/tracker já removeram.

`DecisaoV4` contém `track_id`, `status`, `severity` e o evento original. `status='confirmado'` significa emissão local do motor; `suprimido_cooldown` é diagnóstico de um candidato que começou a aguardar, não notificação. A fachada não altera a flag ativa ou decide qual gravidade prevalece. Abertura e elevação são interpretadas a partir de `tipo_evento` retornado pelo V4.

Eventos, transições e handoffs são consumidos como deltas por passo e suas listas exportadoras são esvaziadas depois da leitura. Estados e referências internas do motor são preservados. Isso impede crescimento da lista histórica sem mudar o mecanismo de confirmação. O motor só é chamado serialmente; não é compartilhado por threads ou câmeras.

## Tratamento de falhas

| Erro | Comportamento |
|---|---|
| `V4InputError` | Rejeita entrada antes de chamar o núcleo; não converte em NORMAL |
| `V4ResponseError` | Rejeita resposta incompatível; invalida a instância; não publica decisões daquele passo |
| `V4ExecutionError` | Invalida a instância após falha interna; encerra o worker e preserva pendências já gravadas |
| Modelo incompatível | Falha antes de consumir vídeo/publicar |
| Hash de pesos divergente, quando configurado | Falha antes de carregar o checkpoint |
| Falha de JPEG | Mantém a política existente de alerta sem evidência |
| Falha transitória AMQP | Outbox preservada e reconexão existente |

Não repetir automaticamente a mesma chamada V4 que falhou: o núcleo é mutável e não fornece transação/rollback. Reiniciar com segmento novo é explícito. Não confundir esse reset com recuperação observada. A outbox não torna atômicos estado em memória, confirmação temporal e enqueue; existe janela residual em crash e possibilidade de nova detecção com UUID novo após reinício.

## Política temporal preservada e limite live

Padrão: normal 0,30, desequilíbrio 0,65, queda 0,50; desequilíbrio 15 consecutivos; queda dez evidências em janela 11; normalidade 15 consecutivos; cooldown 3 s; handoff IoU 0,70/0,30 s; associação de queda IoU 0,50/1,5 s.

Em vídeo, todos os frames são processados sequencialmente; FPS vem do arquivo. Em stream, o slot recente mantém baixa fila no Python e pode descartar frames intermediários. A V4 recebe a sequência processada e timestamp monotônico real. `ENGINE_FPS=15` é o FPS de referência para a regra original de expiração por índice; com amostragem mais lenta, a expiração em tempo de parede muda. Isso é limitação da referência, registrada e monitorada, não corrigida por uma regra paralela no adaptador.

Esta entrega não acrescenta limites artificiais de janela em segundos nem expiração de dois segundos ao V4. Esses limites existiam como proposta no pacote anterior; aqui a prioridade é fidelidade à API fornecida. Lacunas/baixo FPS geram avisos e métricas. Se for necessário mudar a semântica temporal live, alterar e validar uma nova versão do motor, separada da prova de paridade.

## Contratos externos e exceções mínimas

Envelope `schemaVersion=1`, tipos `POSSIVEL_RISCO`/`CAMERA_HEARTBEAT`, chaves, persistência e routing keys foram preservados. Não foram acrescentados campos V4 ao envelope externo: episódio, transição e evidências temporais ficam na auditoria local. `eventKey` continua único por emissão e estável em retries da outbox.

O produtor já emitia ATENCAO, mas o contrato oficial anexado descreve CRITICA. `EMIT_ATTENTION=false` restringe as emissões sem alterar nomes de classes nem converter atenção em crítica. Ativar atenção exige confirmar compatibilidade do consumidor; nenhum suporte do app foi presumido. O padrão `true` preserva a política experimental V4; o ensaio deve permanecer sem publicação até essa conferência.

O n8n chama `http://poolsafe-api:3000/api/v1/internal/alerts` com `X-Internal-Service-Key`, conforme fonte da verdade. Essa credencial não é entregue ao worker. A API continua sendo a fronteira transacional de aceitação: evento anterior à pausa pode ser recusado se processado depois. Não foi introduzida elegibilidade retroativa por timestamp. ACK terminal para `409 ACTIVE_MONITORING_NOT_FOUND` continua exigido no consumidor. O padrão real de tentativas/DLQ precisa ser incorporado ao workflow; os anexos não provam que isso já foi feito.

Exceções fora do motor: adapters do `main`, configuração, instrumentação de transporte, logs/saúde, limites de recursos e build CPU/GPU. São necessárias para entrada/saída corretas e operação em VPS; não mudam fluxos de domínio, banco, API, filas ou aplicativo.

## Verificação e limites da entrega

Foram executados testes locais de lógica e orquestração simulada, incluindo os 47 cenários originais, comparação de paridade em dez sequências de 500 frames, conferência AST das classes, erros de entrada/resposta, logs, saúde e envelope. Os testes históricos usam fixture legada; a orquestração V4 tem teste próprio com captura/modelo/publicador simulados.

Não foram fornecidos pesos reais, workflow n8n exportado, schema completo da API ou acesso à VPS. Build Docker, GPU, RTSP/MJPEG real, RabbitMQ real, chamada autenticada à API e FCM não foram executados. Importar dependências e passar testes simulados não prova desempenho do checkpoint nem meta de oito segundos. Nenhum serviço foi implantado ou mensagem externa publicada.
