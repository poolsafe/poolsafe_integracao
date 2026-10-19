# PoolSafe - produtor Python de integração 2.0.0

Este pacote executa o lado Python: vídeo/câmera -> YOLO11 -> ByteTrack -> confirmação temporal -> RabbitMQ. O n8n, a API Node.js, o MySQL, o FCM e o aplicativo continuam sendo componentes externos. Não é uma implantação completa desses serviços nem uma validação do modelo.

## 1. Comece por aqui

1. Extraia o ZIP no computador ou servidor do colega responsável pelo Docker.
2. Leia este README e `CONTRATO_N8N.md`.
3. Envie o seu **best.pt treinado** ao host Docker. Não é necessário treinar dentro do contêiner.
4. Comece com vídeo, TEST_MODE=true e PUBLISH_RABBIT=false.
5. Depois habilite publicação em exchange/filas exclusivas de teste e configure o consumidor n8n.
6. Só depois teste câmera, heartbeat, reconexão e notificação real.

Os padrões de oito frames, confiança 0,25, 15 frames normais e cooldown de três segundos são parâmetros experimentais, não limites de qualidade comprovados. Não reduzem automaticamente os falsos positivos. O contador usa frames PROCESSADOS consecutivos com o mesmo ID, não todos os frames capturados, e interrompe a sequência ao perder detecção, receber normal ou exceder MAX_INTERVALO_S.

## 2. Arquivos e responsabilidades

| Arquivo | Função |
|---|---|
| main.py | Carrega best.pt, roda YOLO/ByteTrack, gera eventos e coordena encerramento |
| config.py | Lê e valida variáveis de ambiente |
| captura.py | Recebe stream, mantém somente o frame mais recente e reconecta |
| regras.py | Confirma sequências, encerra episódios e aplica cooldown |
| transporte.py | Outbox SQLite e publicação RabbitMQ em thread separada |
| Dockerfile | Constrói a imagem contendo todo o código e as bibliotecas |
| compose.yaml | Modelo de Stack para Portainer/Docker Compose |
| requirements.txt | Dependências fixadas para o perfil de implantação |
| CONTRATO_N8N.md | Payload e obrigações propostas ao consumidor |
| tests/test_core.py e test_pipeline.py | Verificação da lógica e persistência sem serviços reais |

**main.py depende dos outros quatro módulos Python. Não copie apenas esse arquivo.** O Dockerfile copia todos automaticamente. Os comentários explicam decisões e limitações no próprio código.

## 3. Preparar o host e os arquivos

Exemplo de pastas no SERVIDOR onde Docker executa:

```text
/opt/poolsafe/worker/          código extraído do ZIP
/opt/poolsafe/modelos/best.pt  seu modelo treinado
/opt/poolsafe/entrada/teste.mp4
/opt/poolsafe/dados/camera_teste_01/
```

Use SFTP/WinSCP ou o mecanismo já adotado pela equipe para enviar arquivos. O Web editor do Portainer NÃO envia o best.pt ou os módulos Python ao host.

No host, com permissões administrativas para preparar as pastas:

```bash
sudo mkdir -p /opt/poolsafe/modelos /opt/poolsafe/entrada /opt/poolsafe/dados/camera_teste_01
sudo chown -R 10001:10001 /opt/poolsafe/dados/camera_teste_01
```

O contêiner usa UID/GID 10001. O modelo e o vídeo precisam ser legíveis por esse usuário. Montagens de modelo e entrada são somente leitura; não use chmod 777.

Dentro do contêiner:

- modelo: `/modelos/best.pt`;
- vídeo: `/entrada/teste.mp4`;
- dados graváveis: `/dados`.

Não colocar modelos, vídeos ou credenciais na imagem. Nunca carregar arquivos .pt de origem não confiável.

## 4. Construir a imagem e configurar a rede

Na pasta que contém o Dockerfile:

```bash
cd /opt/poolsafe/worker
docker build -t poolsafe-worker:2.0.0 .
```

O build baixa dependências; precisa de acesso aos repositórios de pacotes. A imagem inclui Torch e pode ocupar vários GB. A versão Torch do contêiner difere do ambiente Colab: o carregamento do best real precisa ser testado. Não foi feito build nem teste GPU no ambiente de criação deste pacote.

No Portainer, abra o RabbitMQ e confira a rede Docker e seu alias. O worker e RabbitMQ devem compartilhar uma rede de usuário existente.

- RABBIT_NETWORK: nome real dessa rede;
- RABBIT_HOST: alias/nome do serviço RabbitMQ nessa rede;
- localhost dentro do worker aponta para o PRÓPRIO worker.

A Stack não instala ou substitui o RabbitMQ existente. Não exponha AMQP na Internet. A primeira execução declara exchange Topic durável, duas filas duráveis e bindings. Se já houver recursos homônimos com outros argumentos, o RabbitMQ recusa a declaração: escolha nomes de teste novos ou alinhe a configuração; não apague filas existentes para contornar o erro.

## 5. Primeiro ensaio sem publicar

Portainer -> Stacks -> Add stack. Cole `compose.yaml`, configure HOST_MODEL_DIR, HOST_VIDEO_DIR, HOST_DATA_DIR e RABBIT_NETWORK conforme o host. Defina:

```text
SOURCE_MODE=video
SOURCE=/entrada/teste.mp4
MODEL_VERSION=v6
CAMERA_ID=camera_teste_01
TEST_MODE=true
PUBLISH_RABBIT=false
DEVICE=cpu
```

Clique em Deploy the stack. Abra Containers -> worker -> Logs. O serviço deve registrar hash/classes do modelo, eventos locais e encerramento do arquivo. Não há janela gráfica no contêiner nem MP4 anotado gerado por este pacote.

Os eventos sem publicação aparecem no log; não entram na outbox. O arquivo `/dados/deteccoes.jsonl` contém detecções por frame processado, incluindo frames vazios e detecções sem ID. Logs têm rotação e não são arquivo histórico ilimitado; copie os logs antes de ultrapassar sua retenção em uma avaliação acadêmica. A evidência não é escrita nos logs.

O vídeo é processado no ritmo nominal quando há capacidade; se a inferência for mais lenta, a execução fica mais lenta. Não pula frames de arquivo para compensar atraso. O timestamp `video_time_s` representa posição nominal do arquivo. Não mede latência câmera -> aplicativo. O EOF e uma falha de decodificação podem produzir término semelhante: conferir duração/contagem esperada em ensaio real.

## 6. Publicar no RabbitMQ

Depois de validar o modelo localmente, definir na Stack:

```text
PUBLISH_RABBIT=true
RABBIT_HOST=alias_real_do_rabbit
RABBIT_PORT=5672
RABBIT_USER=usuario_autorizado
RABBIT_PASSWORD=senha_configurada_no_ambiente
RABBIT_VHOST=/
RABBIT_EXCHANGE=poolsafe.teste.eventos.v1
RABBIT_ALERT_QUEUE=poolsafe.teste.alertas.v1
RABBIT_HEARTBEAT_QUEUE=poolsafe.teste.heartbeat.v1
RABBIT_ALERT_KEY=poolsafe.alerta
RABBIT_HEARTBEAT_KEY=poolsafe.heartbeat
```

Não publicar esses valores reais no Git ou em prints. A conta precisa poder declarar exchange, filas e bindings e publicar no virtual host. Para TLS, o código suporta RABBIT_TLS=true e RABBIT_CA_FILE; adicionar essas variáveis e o volume do CA à Stack. A porta TLS deve corresponder ao broker. Não há desativação de validação de certificado.

Uma confirmação do broker não confirma persistência no domínio nem push no telefone. Se n8n estiver parado e a fila existir, o broker pode aceitar a mensagem normalmente.

## 7. Câmera real e heartbeat

Depois do ensaio de vídeo:

```text
SOURCE_MODE=stream
SOURCE=URL_PRIVADA_RTSP_OU_HTTP_MJPEG
TEST_MODE=true
CAMERA_ID=identificador_da_camera
CODIGO_INTEGRACAO=codigo_cadastrado_na_API
```

SOURCE aceita uma URL de stream, não a página de login/interface da câmera. A URL e credenciais devem ficar no ambiente privado. Validar o acesso a partir do contêiner: WARP funcionando no host não garante a mesma rota dentro da rede Docker. Credenciais em variáveis continuam visíveis a administradores do Docker.

Na Stack, trocar restart para `unless-stopped` somente após testes. A captura usa FFmpeg/OpenCV com timeouts de cinco segundos e reconexão em três segundos. A thread de captura mantém um slot: frames antigos no Python são substituídos. Isso não garante ausência de buffers da rede ou da câmera.

O heartbeat é produzido quando o processamento recebe um frame realmente decodificado, aproximadamente a cada 30 segundos. Ele confirma captura, não funcionamento do detector ou recebimento no app. Pode atrasar se a inferência ou o host travarem. Não é enviado em modo vídeo. Há somente um heartbeat pendente em memória e ele expira em 60 segundos. n8n deve validar `observado_em`/`expira_em` e atualizar ultima_comunicacao_em somente para frente.

Ao reconectar, o tracker e o motor reiniciam. Um novo ID/segmento pode originar novo alerta da mesma situação: deduplicação semântica entre reconexões não está resolvida.

## 8. Pausa e controle do monitoramento

Conforme a decisão mais recente, o Python NÃO consulta se o monitoramento está pausado. A captura, a inferência e a publicação continuam. O n8n/API consultam o estado autorizado da sessão e decidem se enviam notificação; durante a pausa o evento deve seguir o registro definido pela equipe sem notificar. Não inventamos endpoint ou tabela para isso.

Desligar a Stack encerra captura e heartbeat; é diferente da pausa no aplicativo. Esta decisão substitui a proposta antiga de arquivo local de controle da análise.

## 9. GPU e múltiplas câmeras

O exemplo funciona por padrão em CPU. Para NVIDIA, o host deve ter driver e NVIDIA Container Toolkit funcionais. Descomente a reserva de dispositivo no compose e defina DEVICE=0. Comente a reserva novamente para CPU. Há outras plataformas GPU, mas este pacote não configura seus runtimes.

Para várias câmeras, replique o serviço com CAMERA_ID, CODIGO_INTEGRACAO, SOURCE e HOST_DATA_DIR exclusivos. Cada processo carrega seus pesos e possui tracker/outbox próprios; não compartilhe SQLite entre processos. As filas podem ser compartilhadas; o consumidor precisa separar por câmera. Consumo de RAM/VRAM aumenta a cada instância: medir antes de aumentar a carga. Não há capacidade máxima de câmeras comprovada.

## 10. Parâmetros importantes

| Variável | Padrão | Significado |
|---|---|---|
| FRAMES_PARA_ALERTA | 8 | Frames processados consecutivos de queda com mesmo ID |
| CONF | 0.25 | Limiar da inferência, não precisão global |
| IOU | 0.70 | Parâmetro da supressão de caixas, não IoU de avaliação |
| FRAMES_NORMAIS_ENCERRAR | 15 | Normais consecutivos para rearmar o mesmo ID |
| COOLDOWN_S | 3 | Intervalo mínimo global por câmera entre emissões |
| MAX_INTERVALO_S | 0.5 | Máxima lacuna temporal entre frames consecutivos |
| LOST_RESET_S | 2 | Tempo sem ID para descartar estado |
| ALERT_TTL_S | 120 | Validade do alerta desde a decodificação do frame de confirmação |
| INCLUDE_EVIDENCE | false | Anexar JPEG privado em base64; requer contrato consumidor |
| MAX_PENDING_EVENTS | 10000 | Limite de pendências; ao atingir, o serviço falha explicitamente |

Os números não são critérios de aceite do TCC. Cooldown pode suprimir outra pessoa ou evento distinto; identidades fragmentadas ainda podem repetir alertas. Oito frames NÃO são oito segundos. Em stream, os frames do contador podem não ser adjacentes no vídeo original porque o slot descarta frames; registrar a taxa efetiva e medir o efeito dessa estratégia.

Alertas expirados são removidos da outbox com registro de aviso. Isso é uma política operacional proposta para não entregar avisos muito antigos; o log não contém o payload completo do evento expirado. Caso o domínio exija arquivo histórico de todos os eventos, implementar arquivo morto/armazenamento separado com retenção definida antes do uso final.

Outbox é armazenamento local de transporte, não substitui MySQL. Contém eventos sensíveis quando evidências estão habilitadas. Proteja volume/backups; reduzir MAX_PENDING_EVENTS quando habilitar imagens. O limite de eventos não é uma quota total de disco; SQLite pode conservar espaço livre no arquivo, e é preciso monitorar volume. Somente o sistema consumidor define a retenção final de evidências.

## 11. Trocar best.pt

1. Preserve a versão anterior fora da pasta montada em produção.
2. Pare o worker antes de substituir o arquivo.
3. Envie o novo arquivo como `/opt/poolsafe/modelos/best.pt`.
4. Atualize MODEL_VERSION e reinicie o serviço.
5. Confira SHA-256 e classes no log.

A troca de pesos não exige rebuild. Mudança de código/dependências exige nova imagem. Pesos precisam ter pessoa_normal/pessoa_queda, ou configurar NORMAL_CLASS/RISK_CLASS no ambiente (adicionar à Stack se necessário). Não renomear classes só para forçar um modelo semanticamente incompatível a iniciar.

## 12. Testes e limites de validação

```bash
python -m unittest discover -s tests -v
python -m compileall -q .
```

Os 12 testes passaram. Os testes do núcleo não dependem de YOLO/Pika reais; o ensaio do pipeline usa captura e modelo simulados, não inferência real. Foram verificados confirmação, ausência, lacuna temporal, IDs, rearmamento, cooldown, persistência, limite da outbox, heartbeat mais recente, propriedades de publicação e preservação em falha do broker.

Não foram executados aqui: build Docker, instalação completa do requirements, carregamento do best, inferência GPU/CPU real, acesso RTSP/MJPEG, RabbitMQ real, workflow n8n, MySQL ou FCM. Não há comprovação de meta de oito segundos ou de redução de falsos positivos.

## 13. Roteiro de aceitação da integração

- Arquivo: conferir detecções/ausências e confirmar que não há heartbeat.
- Broker: publicar em filas de teste e verificar o event_id no n8n.
- Falha: parar broker, gerar evento, reiniciar broker e conferir preservação/reenvio antes do TTL.
- Duplicação: reprocessar mesmo event_id e confirmar apenas um efeito no domínio/notificação.
- Câmera: testar frame válido, perda, reconexão e expiração de conectividade.
- Pausa: comprovar que Python continua publicando e que n8n não notifica.
- App: medir entrega e estados foreground/background/encerrado conforme implementação.
- Avaliação: anotar início real do evento, perdas e falsos alertas; informar cobertura junto da latência.

Para a banca, vídeo bruto processado na hora demonstra inferência sobre uma gravação e o restante do fluxo. Um evento artificial demonstra transporte, sem comprovar detecção. Alinhar a forma de demonstração com a professora.

## 14. Referências de implementação

- Ultralytics tracking: https://docs.ultralytics.com/modes/track/
- Pika publisher confirms: https://pika.readthedocs.io/en/stable/examples/blocking_delivery_confirmations.html
- RabbitMQ confirms: https://www.rabbitmq.com/docs/confirms
- RabbitMQ TTL: https://www.rabbitmq.com/docs/ttl

Arquitetura-base: Fonte da Verdade de 03/10/2026, complementada pelas decisões posteriores sobre Topic e pausa controlada pelo n8n. Payload e nomes são proposta de integração, não um contrato já ratificado pelo backend.
