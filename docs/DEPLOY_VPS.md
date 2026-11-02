# Implantação e dimensionamento na VPS

## 1. Ambiente e pré-requisitos

A fonte da verdade informa Ubuntu 24.04 LTS, Docker Engine/Compose, Portainer, WARP, rede `poolsafe-network`, broker RabbitMQ 4.3.6 e API/n8n existentes. Não informa quantidade disponível de vCPUs, RAM ou modelo de GPU; não presumir GPU. A câmera de teste validada é HTTP/MJPEG via WARP/Orange Pi. A câmera RTSP definitiva continua pendente.

Não criar outro broker, banco, API ou Tunnel. O worker não expõe portas. Ele precisa resolver o host da câmera dentro do contêiner e alcançar o broker pela rede interna; acesso do host via WARP não garante automaticamente roteamento/DNS no namespace Docker.

Na VPS, inventariar antes de escolher orçamento:

```bash
docker version
docker compose version
docker network inspect poolsafe-network
docker stats --no-stream
free -h
df -h
nvidia-smi
```

`nvidia-smi` é opcional e pode não existir em VPS sem GPU. Não instalar drivers ou alterar WARP automaticamente. Validar conectividade privada usando um diagnóstico controlado, sem imprimir SOURCE com credenciais. TLS AMQP é opcional no perfil atual da rede interna; se habilitado, montar CA e usar porta configurada na infraestrutura. Não expor 5672 ou interfaces administrativas ao público.

## 2. Dependências e imagem CPU

O núcleo V4 usa somente biblioteca padrão. Mantidas as versões diretas da integração: Ultralytics 8.4.173, torch 2.6.0, torchvision 0.21.0, OpenCV 4.11.0.86, numpy 2.1.3, pika 1.3.2 e lap 0.5.12. Python 3.11 permanece na imagem. O ambiente difere do Colab: testar o checkpoint real é obrigatório.

O Dockerfile instala por padrão wheels CPU de torch/torchvision no índice oficial antes das demais dependências, reduzindo bibliotecas CUDA desnecessárias. A base ainda é uma tag: após validar, fixar o digest da base e da imagem final no processo de release. As versões transitivas não foram inventadas; registrar `pip freeze`, SBOM e hashes da imagem efetivamente construída. Não executar pip install durante inicialização ou por câmera.

Build local no diretório do pacote:

```bash
docker build --build-arg BUILD_REVISION="$(git rev-parse HEAD)" -t poolsafe-worker:v4-cpu .
docker run --rm --network none poolsafe-worker:v4-cpu python -c "import torch, torchvision, cv2, pika, ultralytics, engine_adapter; print('imports-ok')"
docker run --rm --network none poolsafe-worker:v4-cpu python -m pip freeze
```

Os comandos são instruções, não resultados desta entrega. CI mantém a publicação controlada via GHCR e inclui revisão de build. A implantação no Portainer continua manual. Não executar workflow/push para produção somente por receber este pacote.

## 3. Diretórios e configuração

Criar diretórios somente para a câmera escolhida, preservando dados existentes:

```bash
sudo install -d -m 0755 /opt/poolsafe/modelos/releases/modelo-002
sudo install -d -m 0755 /opt/poolsafe/entrada
sudo install -d -m 0750 -o 10001 -g 10001 /opt/poolsafe/dados/camera_teste_01
cp .env.example stack.env
chmod 600 stack.env
```

Copiar manualmente o `best.pt` de três classes para o diretório de modelo, com leitura pelo UID 10001, e o vídeo reservado para entrada. Calcular `sha256sum` no arquivo e preencher `MODEL_SHA256`. A verificação ocorre antes de carregar os pesos quando o valor foi configurado. Usar checkpoint confiável; hash isolado não certifica procedência.

No `stack.env`, editar:

- `WORKER_IMAGE`: imagem local do ensaio ou referência GHCR/digest realmente publicado;
- diretórios de host e `MODEL_VERSION`/`MODEL_SHA256`;
- `SOURCE_MODE=video`, `SOURCE=/entrada/teste.mp4` para primeiro ensaio;
- `TEST_MODE=true`, `PUBLISH_RABBIT=false`, `WORKER_RESTART=no`;
- câmera/código de integração e usuário/senha AMQP somente quando publicar;
- limites de CPU/RAM conforme espaço disponível.

Não substituir segredo por exemplo do documento. `SOURCE` com usuário/senha fica no arquivo privado, não no Git ou em comandos registrados em histórico. `stack.env` é parâmetro do Compose via `--env-file`; o Compose encaminha explicitamente as variáveis. No Portainer, importar as variáveis privadas na Stack conforme o fluxo já existente.

Validar sem imprimir os valores resolvidos:

```bash
docker compose --env-file stack.env config --quiet
docker compose --env-file stack.env up -d
docker compose --env-file stack.env logs --tail 100 poolsafe
docker compose --env-file stack.env ps
```

O modo vídeo termina ao fim do arquivo: um contêiner encerrado nesse ensaio não é uma falha de disponibilidade. Não usar restart permanente para vídeo, pois repetiria a reprodução. A imagem usa UID 10001; falta de permissão no volume impede a outbox/saúde de funcionar.

## 4. Homologação do checkpoint real

Conferir no ensaio: hash do modelo, classes, política V4, inferência sem exceção, eventos locais, arquivo `resumo.json`, JSONL de eventos e reprodução dos casos de referência. A ausência de alerta em vídeo normal não basta: incluir queda, recuperação, nova queda, mudança de ID e controles negativos.

Depois, testar `SOURCE_MODE=stream` com câmera de teste pela rota privada. Manter publicação desabilitada. Observar `worker.metrics`, idade dos frames, FPS efetivo, memória e `health.json`. Só então validar publicação numa câmera/workflow de teste com destinatários controlados. `TEST_MODE=true` é informativo para o consumidor; `ALLOW_TEST_PUBLISH=true` é a liberação explícita exigida pelo worker. Não basta trocar esse campo para isolar destinatários.

Confirmar suporte a ATENCAO no contrato externo. Se só CRITICA for aceita, configurar `EMIT_ATTENTION=false`. Não renomear ou elevar artificialmente o evento para contornar rejeição. A API/n8n devem usar sua chave interna já definida; a chave não vai para o Python.

## 5. GPU opcional

Usar somente se há GPU NVIDIA real, driver e NVIDIA Container Toolkit validados no host. A imagem CPU não passa a executar CUDA apenas com `DEVICE=0`.

Exemplo de build CUDA da família compatível com torch 2.6, a homologar no driver real:

```bash
docker build --build-arg TORCH_INDEX_URL=https://download.pytorch.org/whl/cu124 --build-arg BUILD_REVISION="$(git rev-parse HEAD)" -t poolsafe-worker:v4-cu124 .
docker run --rm --gpus all --network none poolsafe-worker:v4-cu124 python -c "import torch; print(torch.cuda.is_available()); print(torch.version.cuda)"
docker compose --env-file stack.env -f compose.yaml -f compose.gpu.yaml config --quiet
docker compose --env-file stack.env -f compose.yaml -f compose.gpu.yaml up -d
```

Preencher `WORKER_IMAGE` com a variante GPU antes do último comando. Não publicar a imagem CPU sob tag ambígua de GPU. O worker falha explicitamente se CUDA for solicitada e indisponível. A medição de inferência sincroniza o dispositivo, introduzindo custo de instrumentação; não é cobrança de serviço.

## 6. Recursos, carga e escalabilidade

O Compose inclui valores iniciais **de ensaio**, 2 CPUs e 3 GiB por worker, não um requisito medido ou garantia de suficiência. `TORCH_THREADS=2`, interop 1 e OpenCV 1 evitam concorrência CPU ilimitada. `OMP_NUM_THREADS`, `MKL_NUM_THREADS` e `OPENBLAS_NUM_THREADS` seguem o orçamento configurado. Ajustar threads e cotas conjuntamente; aumentar threads em cada câmera pode piorar a latência global.

O caminho por câmera tem três atividades: captura em thread com slot recente; inferência e V4 serializadas no processo principal; publicação em thread própria com outbox. Não há fila ilimitada de frames no Python. Buffers anteriores, da câmera/rede/FFmpeg, ainda podem atrasar vídeo. Em arquivo todos os frames são processados; em live medir os saltos em `capture_skipped`.

Sob aumento de carga:

1. Medir uma câmera no hardware real, incluindo aquecimento, CPU/RAM/VRAM e p95 por etapa.
2. Reservar recursos para MySQL, API, n8n, RabbitMQ e sistema; não atribuir toda a VPS ao worker.
3. Aumentar câmeras uma de cada vez com volumes/nome de contêiner/código próprios. Um proprietário ativo por câmera; não usar `--scale` para duplicar o mesmo stream.
4. Comparar p95/idade dos frames, descarte, backlog, memória e erros. Pausar expansão ao degradar; não reduzir limiar ou contagem automaticamente para compensar hardware.
5. Manter pesos, tracker e V4 fixos no ensaio. Alterar resolução ou stride exige nova validação de desempenho e qualidade.
6. Se a GPU ficar ociosa entre muitas instâncias, avaliar futuramente scheduler/inferência compartilhada com estado por câmera, ordenação e backpressure. Isso não está implementado nesta entrega.

Não foi estabelecido número máximo de câmeras. O contador V4 mede observações processadas; FPS baixo aumenta tempo para confirmação. `ENGINE_FPS` também participa da expiração original por índice, portanto não promete duração real fixa. A meta de oito segundos exige instrumentação da ocorrência até o aparelho, não apenas `inference_ms` ou `v4_ms`.

## 7. Saúde, parada e rollback

`healthcheck.py` lê estado local atualizado pelo worker e verifica progresso de captura/inferência; não publica heartbeat, não toca API e não abre porta. Healthcheck do Docker não reinicia sozinho um contêiner unhealthy no Compose. Adotar alarme externo; watchdog/restart por saúde exigem configuração operacional separada. `WORKER_RESTART=no` é padrão de ensaio; `on-failure:3` pode ser candidato após homologação, sem esconder erros persistentes de modelo/contrato.

Para parar de forma controlada:

```bash
docker compose --env-file stack.env stop -t 45 poolsafe
```

Verificar pendências da outbox e motivo de encerramento. Não apagar `outbox.sqlite3`, WAL ou SHM. Backups consistentes de SQLite devem usar a API de backup ou parada controlada, nunca cópia isolada do arquivo principal durante escrita. Não compartilhar a mesma outbox entre réplicas.

Rollback mantém volume, mensagens e eventKeys e troca somente imagem/pesos compatíveis. O estado temporal em memória reinicia: isso pode originar nova emissão da mesma ocorrência; não existe reidentificação ou exatamente uma vez de ponta a ponta. Aplicar rollback em canário e observar efeitos, sem esvaziar filas como atalho.

## 8. Referências oficiais

- Docker Compose, serviços/limites/healthcheck: https://docs.docker.com/reference/compose-file/services/
- PyTorch, versões CPU/CUDA: https://pytorch.org/get-started/previous-versions/
- PyTorch, threads CPU: https://docs.pytorch.org/docs/stable/generated/torch.set_num_threads.html

Documentação corrente pode conter versões posteriores às dependências fixadas. As instruções de build e implantação foram preparadas, mas não executadas no servidor nesta entrega.
