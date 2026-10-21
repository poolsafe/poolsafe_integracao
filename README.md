# PoolSafe - produtor Python de integração 2.0.0

Este pacote executa o lado Python: vídeo/câmera -> YOLO11 -> ByteTrack -> confirmação temporal -> RabbitMQ. O n8n, a API Node.js, o MySQL, o FCM e o aplicativo continuam sendo componentes externos. Não é uma implantação completa desses serviços nem uma validação do modelo.

## 1. Comece por aqui

1. Leia este README e `CONTRATO_N8N.md`.
2. Envie o seu **best.pt treinado** ao host Docker. Não é necessário treinar dentro do contêiner.
3. Comece com vídeo, TEST_MODE=true e PUBLISH_RABBIT=false.
4. Depois habilite publicação em exchange/filas exclusivas de teste e configure o consumidor n8n.
5. Só depois teste câmera, heartbeat, reconexão e notificação real.

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

No Portainer, configure a rede externa oficial `poolsafe-network`. O worker e RabbitMQ compartilham essa rede:

- DOCKER_NETWORK: `poolsafe-network`;
- RABBIT_HOST: `rabbitmq` (alias do serviço na rede);
- Vhost: `poolsafe`.

A Stack não cria nem altera o RabbitMQ existente. O usuário produtor `poolsafe_python` possui permissões estritas para publicar na exchange `poolsafe.events` (`direct`, durável). O produtor **não declara** exchanges, filas ou bindings e não administra DLQ.

## 5. Primeiro ensaio sem publicar

Portainer -> Stacks -> Add stack. Cole `compose.yaml`, configure HOST_MODEL_DIR, HOST_VIDEO_DIR e HOST_DATA_DIR conforme o host. Defina:

```text
SOURCE_MODE=video
SOURCE=/entrada/teste.mp4
MODEL_VERSION=candidato
CAMERA_ID=camera_teste_01
TEST_MODE=true
ALLOW_TEST_PUBLISH=false
PUBLISH_RABBIT=false
DEVICE=cpu
```

Clique em Deploy the stack. Abra Containers -> worker -> Logs. O serviço deve registrar hash/classes do modelo, eventos locais e encerramento do arquivo. Não há janela gráfica no contêiner nem MP4 anotado gerado por este pacote. O `MODEL_VERSION=candidato` reflete uma versão claramente provisória até identificarmos e atestarmos o modelo real.

Os eventos sem publicação aparecem no log; não entram na outbox. O arquivo `/dados/deteccoes.jsonl` contém detecções por frame processado, incluindo frames vazios e detecções sem ID. Logs do Docker utilizam rotação (`max-size: 10m`, `max-file: 3`). A evidência não é escrita nos logs.

O vídeo é processado no ritmo nominal quando há capacidade; se a inferência for mais lenta, a execução fica mais lenta. Não pula frames de arquivo para compensar atraso. O timestamp `video_time_s` representa posição nominal do arquivo. Não mede latência câmera -> aplicativo. O EOF e uma falha de decodificação podem produzir término semelhante: conferir duração/contagem esperada em ensaio real.

## 6. Publicar no RabbitMQ

Depois de validar o modelo localmente, definir na Stack:

```text
PUBLISH_RABBIT=true
ALLOW_TEST_PUBLISH=true
CODIGO_INTEGRACAO=codigo_cadastrado_na_api
RABBIT_HOST=rabbitmq
RABBIT_PORT=5672
RABBIT_USER=poolsafe_python
RABBIT_PASSWORD=senha_configurada_no_ambiente
RABBIT_VHOST=poolsafe
RABBIT_EXCHANGE=poolsafe.events
RABBIT_ALERT_KEY=risk.detected
RABBIT_HEARTBEAT_KEY=camera.heartbeat
ALERT_TTL_S=0
```

Não publicar esses valores reais no Git ou em prints. Para publicar em modo de teste (`TEST_MODE=true`), a flag `ALLOW_TEST_PUBLISH=true` é estritamente obrigatória, garantindo que o operador configurou destinos e câmeras de teste controlados.

O produtor publica alertas com persistência (`delivery_mode=2`), confirmação (`confirm_delivery`) e `mandatory=True`. Uma confirmação do broker não confirma persistência no domínio nem push no telefone. Se n8n estiver parado e a fila existir, o broker pode aceitar a mensagem normalmente.

## 7. Câmera real e heartbeat

Depois do ensaio de vídeo:

```text
SOURCE_MODE=stream
SOURCE=URL_PRIVADA_RTSP_OU_HTTP_MJPEG
TEST_MODE=true
ALLOW_TEST_PUBLISH=true
CAMERA_ID=identificador_operacional
CODIGO_INTEGRACAO=codigo_cadastrado_na_API
```

SOURCE aceita uma URL de stream, não a página de login/interface da câmera. A URL e credenciais devem ficar no ambiente privado. Validar o acesso a partir do contêiner.

Na Stack, trocar restart para `unless-stopped` somente após testes. A captura usa FFmpeg/OpenCV com timeouts de cinco segundos e reconexão em três segundos. A thread de captura mantém um slot para inferência: frames antigos no Python são substituídos.

O heartbeat é desacoplado da inferência: é gerado por callback a partir da decodificação confirmada de frames reais pelo leitor de stream, aproximadamente a cada 30 segundos, com validade de 60 segundos. Ele confirma captura ativa recente, não funcionamento da inferência ou do aplicativo. Não é emitido em modo vídeo nem na ausência de novos frames decodificados. Apenas o heartbeat mais recente fica retido em memória. A API adota janela de 90 segundos para determinar perda de comunicação.

Ao reconectar, o tracker e o motor reiniciam. Um novo ID/segmento pode originar novo alerta da mesma situação: deduplicação semântica entre reconexões não está resolvida.

## 8. Pausa e controle do monitoramento

O Python NÃO consulta se o monitoramento está pausado. A captura, a inferência e a publicação continuam de forma autônoma.

A API é a autoridade de negócio:
- Exige exatamente uma sessão de monitoramento `ATIVO` vinculada à câmera para criar alerta e notificar;
- Sem monitoramento ativo, a API não cria alerta nem envia FCM e registra ocorrência operacional;
- Se a API retornar HTTP `409` com código `ACTIVE_MONITORING_NOT_FOUND`, o consumidor n8n deve tratar como recusa de negócio legítima e **não deve retentar em loop** até uma suposta retomada;
- Não há armazenamento local de detecções garantido durante a pausa.

## 9. GPU e múltiplas câmeras

O exemplo funciona por padrão em CPU. Para NVIDIA, o host deve ter driver e NVIDIA Container Toolkit funcionais. Descomente a reserva de dispositivo no compose e defina DEVICE=0. Comente a reserva novamente para CPU.

Para várias câmeras, replique o serviço com CAMERA_ID, CODIGO_INTEGRACAO, SOURCE e HOST_DATA_DIR exclusivos. Cada processo carrega seus pesos e possui tracker/outbox próprios; não compartilhe SQLite entre processos. As filas são compartilhadas na infraestrutura oficial.

## 10. Parâmetros e Limitações Confirmadas

| Variável | Padrão | Significado |
|---|---|---|
| FRAMES_PARA_ALERTA | 8 | Frames processados consecutivos de queda com mesmo ID |
| CONF | 0.25 | Limiar da inferência, não precisão global |
| IOU | 0.70 | Parâmetro da supressão de caixas, não IoU de avaliação |
| FRAMES_NORMAIS_ENCERRAR | 15 | Normais consecutivos para rearmar o mesmo ID |
| COOLDOWN_S | 3 | Intervalo mínimo global por câmera entre emissões |
| MAX_INTERVALO_S | 0.5 | Máxima lacuna temporal entre frames consecutivos |
| LOST_RESET_S | 2 | Tempo sem ID para descartar estado |
| ALERT_TTL_S | 0 | Validade do alerta em segundos (0 = sem expiração automática no produtor) |
| ALLOW_TEST_PUBLISH | false | Liberação explícita para publicar no broker em modo de teste |
| INCLUDE_EVIDENCE | false | Anexar JPEG privado em base64 (falha secundária não impede o alerta) |
| MAX_PENDING_EVENTS | 10000 | Limite de pendências na outbox; ao atingir, falha explicitamente |

### Limitações Experimentais Confirmadas:
1. **Cadência de captura vs. lacuna temporal**: Em ambientes com taxa de inferência ou amostragem mais lenta (intervalos de 0,8 segundo entre frames processados), a sequência temporal é interrompida pela lacuna máxima configurada de 0,5 segundo (`MAX_INTERVALO_S=0.5`), impedindo atingir os 8 frames necessários para o alerta.
2. **Supressão por Cooldown Global**: O cooldown de 3,0 segundos é aplicado por câmera (`Motor.last_alert`). Se duas pessoas sofrerem eventos concorrentes, a segunda pessoa pode ter sua confirmação suprimida durante a vigência do cooldown da câmera, permanecendo sem alerta.

Por padrão (`ALERT_TTL_S=0`), não há expiração automática no SQLite nem envio da propriedade `expiration` ao RabbitMQ. Caso configurado TTL positivo, alertas vencidos são removidos da outbox com log de aviso. A outbox é armazenamento local transitório de transporte, com preservação de pendentes em caso de indisponibilidade do broker.

## 11. Trocar best.pt

1. Preserve a versão anterior fora da pasta montada em produção.
2. Pare o worker antes de substituir o arquivo.
3. Envie o novo arquivo como `/opt/poolsafe/modelos/best.pt`.
4. Atualize MODEL_VERSION e reinicie o serviço.
5. Confira SHA-256 e classes no log.

A troca de pesos não exige rebuild. Pesos precisam conter `pessoa_normal` e `pessoa_queda`.

## 12. Testes e limites de validação

```bash
python -m unittest discover -s tests -v
python -m compileall -q .
```

21 testes automatizados executados com sucesso:
- Regras temporais: persistência de frames, quebra por ausência, lacuna temporal, segregação de IDs, rearmamento e supressão de cooldown;
- Transporte e persistência: reabertura SQLite, preservação de payload e identidade, limite com erro explícito, publicação mandatory e confirmação;
- Topologia RabbitMQ: ausência estrita de declarações ativas de exchange/filas/bindings pelo produtor;
- Expiração e TTL: risco sem TTL por padrão (omissão da propriedade AMQP `expiration`);
- Evidência isolada: falhas de redimensionamento, codificação ou excesso de tamanho não descartam o alerta;
- Desacoplamento do heartbeat: callback atrelado à captura confirmada, sem emissão periódica inventada e sem envio em modo vídeo;
- Isolamento de testes: validação estrita bloqueando publicação de teste sem `ALLOW_TEST_PUBLISH=true` e sem `CODIGO_INTEGRACAO`;
- Compatibilidade: adaptação transparente de registros legados da outbox;
- Resiliência de transporte: parada coordenada do processo principal com motivo sanitizado e preservação da outbox em erros irrecuperáveis (payloads corrompidos ou autenticação recusada), mantendo reconexão automática contínua em falhas transitórias do broker.

**Limitações de validação:** Os testes utilizam simulações sem modelo, câmera, broker ou banco reais. Não foram validados em ambiente de produção: carregamento de pesos proprietários `best.pt`, acurácia do modelo, performance GPU, decodificação RTSP/H.264 ao vivo, consumo de fila pelo n8n, MySQL ou entrega push FCM no aplicativo.

## 13. Roteiro de aceitação da integração

- Arquivo: conferir detecções/ausências e confirmar ausência de heartbeat em vídeo;
- Broker: publicar com `ALLOW_TEST_PUBLISH=true` e verificar `eventKey` no n8n;
- Falha: parar broker, gerar evento, reiniciar broker e conferir preservação da outbox e entrega única;
- Duplicação: reprocessar mesmo `eventKey` e confirmar idempotência na API;
- Câmera: validar captura de stream, reconexão e emissão do heartbeat de conectividade;
- Pausa: comprovar que o Python publica normalmente e que o n8n descarta/confirma `409 ACTIVE_MONITORING_NOT_FOUND` sem loop de retentativas;
- App: medir latência total ponta a ponta.

## 14. Referências de implementação

- Contrato de alertas da API PoolSafe: `POST /api/v1/internal/alerts`
- Topologia oficial RabbitMQ: exchange `poolsafe.events` (`direct`), vhost `poolsafe`
- Ultralytics tracking: https://docs.ultralytics.com/modes/track/
- Pika publisher confirms: https://pika.readthedocs.io/en/stable/examples/blocking_delivery_confirmations.html
- RabbitMQ confirms & TTL: https://www.rabbitmq.com/docs/confirms
