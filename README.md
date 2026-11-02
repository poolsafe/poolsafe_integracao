# PoolSafe — integração V4 para VPS

O worker usa exclusivamente o núcleo temporal V4 extraído do script fornecido. O motor recebe todas as caixas YOLO/ByteTrack e decide confiança por classe, persistência, elevação, recuperação, handoff e associação. Não é um serviço HTTP.

- [Refatoração, estrutura e contratos](docs/REFATORACAO_V4.md)
- [Implantação na VPS, CPU/GPU e escala](docs/DEPLOY_VPS.md)
- [Logs JSON e monitoramento](docs/LOGS_MONITORAMENTO.md)
- [Homologação do stream e capturas anotadas](docs/HOMOLOGACAO_STREAM.md)
- [Contrato de transporte existente](CONTRATO_N8N.md)

## Início

1. Executar `python -m unittest discover -s tests -v`.
2. Construir a imagem conforme o guia VPS; testar os pesos reais na imagem.
3. Copiar `.env.example` para arquivo privado de ambiente e preencher caminhos/imagem.
4. Iniciar com vídeo, `TEST_MODE=true` e `PUBLISH_RABBIT=false`.
5. Homologar câmera via rede privada antes de habilitar publicação em destino controlado.

## Política padrão

Normal 0,30; desequilíbrio 0,65 e 15 consecutivos; queda 0,50 e dez evidências em até onze frames; recuperação 15 normais; cooldown de abertura 3 s. Elevação usa evidência própria e não é bloqueada pelo cooldown. `CONF`, `FRAMES_PARA_ALERTA` e `LOST_RESET_S` do worker antigo não governam esta versão. Não há fallback para duas classes ou para o motor antigo.

`EMIT_ATTENTION=true` preserva as duas classes emissoras da validação V4. Se o consumidor aceitar somente CRITICA, configurar `EMIT_ATTENTION=false` antes de publicar. Atenção não é convertida em crítica. O envelope externo foi preservado, sem novos campos de episódio.

## Operação

Uma câmera por processo/volume, uma inferência por frame e execução serial do motor. Captura e publicação continuam em threads separadas. O modo live pode descartar frames; o modo vídeo não pula frames. Logs JSON incluem tempos e alertas de amostragem; `healthcheck.py` verifica progresso local sem criar endpoints.

A API/n8n permanecem responsáveis por sessão, pausa, destinatários e FCM. A fonte da verdade confirma `X-Internal-Service-Key` somente no n8n→API. O worker não conhece essa chave. Não houve alteração de banco, app, filas ou workflows.

## Validação

Os testes usam fixtures e não acessam câmera, broker, API ou FCM. Os 47 cenários originais estão em `tests/test_reference.py`; `tests/test_boundaries.py` confere proveniência por AST, falhas, logs, saúde e contratos de entrada. A fixture temporal legada serve apenas para regressão histórica e é excluída da imagem. Os guias distinguem implementação testada localmente de homologação real pendente.

Para explicar detecções sem evento, o modo `homologation` oferece auditoria opcional
em `runs/<run_id>/diagnostico_frames.jsonl`: habilitar
`HOMOLOGATION_DIAGNOSTICS_ENABLED=true` (padrão `false`), com limite
`HOMOLOGATION_DIAGNOSTICS_MAX_FRAMES=600`. Inclui frames vazios, caixas sem ID,
respostas temporais e comparação pós-NMS/saída do tracker da mesma inferência.
Sampling gaps são agregados ao resumo de 100 frames. Consulte o
[guia de homologação](docs/HOMOLOGACAO_STREAM.md) para coleta e limites técnicos.

Nenhum peso, vídeo ou segredo real acompanha este pacote. GPU, build Docker e integração real na VPS exigem validação no ambiente alvo. O motor não confirma afogamento nem comprova a meta de oito segundos.
