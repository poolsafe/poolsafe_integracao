# Contrato de Integração Python -> RabbitMQ -> n8n -> API

Este documento define o contrato oficial de comunicação do produtor Python integrado à infraestrutura existente do PoolSafe.
O n8n consome as mensagens publicadas no RabbitMQ e as encaminha para a API:
`POST http://poolsafe-api:3000/api/v1/internal/alerts`

## 1. Topologia RabbitMQ Oficial

| Item | Configuração Oficial | Papel |
|---|---|---|
| Rede Docker externa | `poolsafe-network` | Rede compartilhada entre contêineres |
| Host e porta AMQP | `rabbitmq:5672` | Endpoint do broker RabbitMQ |
| Vhost | `poolsafe` | Virtual host isolado |
| Usuário produtor | `poolsafe_python` | Permissão estrita de publicação na exchange (sem gestão de filas/bindings) |
| Exchange de eventos | `poolsafe.events` | Tipo `direct`, durável |
| Routing key de risco | `risk.detected` | Encaminhamento para fila de alertas |
| Routing key de heartbeat | `camera.heartbeat` | Encaminhamento para fila de conectividade |
| Fila de risco existente | `poolsafe.risk` | Quorum queue consumida pelo workflow de alertas n8n |
| Fila de heartbeat existente | `poolsafe.heartbeat` | Classic queue para monitoramento de conectividade da câmera |

> **Importante**: O produtor Python NÃO realiza declarações ativas de exchange, filas ou bindings. A topologia é provisionada pela infraestrutura. Dead Letter Queues (DLQ) são administradas pelo broker e pelo n8n; o Python não publica diretamente nelas.

## 2. Envelope de Evento de Risco

Publicado na exchange `poolsafe.events` com routing key `risk.detected`.

```json
{
  "schemaVersion": "1",
  "eventKey": "c1f7a2d8-4f8e-4a6c-9c71-2b0e9f3b1a11",
  "cameraCode": "CAM_PISCINA_01",
  "type": "POSSIVEL_RISCO",
  "severity": "CRITICA",
  "detectedAt": "2026-10-21T01:15:30.125Z",
  "confidence": 0.82,
  "modelVersion": "candidato",
  "technicalData": {
    "test": true,
    "source": "video",
    "runId": "a8e932b1-12c4-4b90-9ef1-4b7194ad825e",
    "segment": 0,
    "trackId": 7,
    "processedFrame": 150,
    "videoTimeSeconds": 4.9667,
    "modelSha256": "7e75a1664e219d195e6c57782dad7e997f121cd756eb0c86e556fe85074f3331"
  },
  "evidence": {
    "mimeType": "image/jpeg",
    "base64": "/9j/4AAQSkZJRgABAQAAAQABAAD..."
  }
}
```

### Regras do Envelope de Risco:
- `schemaVersion`: string `"1"`.
- `eventKey`: UUID estável gerado uma única vez, reutilizado em reenvios e usado como AMQP `message_id`. A API possui idempotência por `eventKey`.
- `cameraCode`: chave pública de integração da câmera (`codigo_integracao` cadastrado no domínio). Não é o ID numérico/interno de banco de dados. Exigido obrigatoriamente se a publicação estiver habilitada.
- `type`: `"POSSIVEL_RISCO"`.
- `severity`: `"CRITICA"`.
- `detectedAt`: instante de decodificação UTC com precisão de milissegundos e sufixo `Z` do frame que completou a confirmação temporal. Preservado em retries.
- `confidence`: valor numérico float de confiança da detecção.
- `modelVersion`: string identificadora da versão dos pesos.
- `technicalData`: metadados técnicos de tracking e execução (limitado a 16 KiB, sem imagem, sem URL ou credenciais). `videoTimeSeconds` é `null` em streams ao vivo.
- `evidence`: objeto opcional com `mimeType` e `base64`. É **omitido** se o envio de evidência estiver desabilitado, ou se houver falha de redimensionamento/codificação JPEG ou se ultrapassar 250.000 bytes.
- O payload NÃO contém dados de usuários, IDs de monitoramento ou tokens FCM (responsabilidade do domínio/API).

## 3. Envelope de Heartbeat de Conectividade

Publicado na exchange `poolsafe.events` com routing key `camera.heartbeat`.

```json
{
  "schemaVersion": "1",
  "eventKey": "7b83c180-87a4-43ea-9c09-fa936a58fbe8",
  "type": "CAMERA_HEARTBEAT",
  "cameraCode": "CAM_PISCINA_01",
  "observedAt": "2026-10-21T01:15:30.000Z",
  "expiresAt": "2026-10-21T01:16:30.000Z",
  "technicalData": {
    "test": true
  }
}
```

### Regras do Heartbeat:
- Emitido a partir da decodificação confirmada de frames reais pela thread de captura (`StreamReader`), independente da duração ou avanço da inferência YOLO.
- Nunca é emitido em modo vídeo (`source_mode=video`).
- Não é emitido na ausência de novos frames ou se a conexão de stream for interrompida.
- Somente o heartbeat mais recente permanece retido em memória aguardando envio.
- Intervalo nominal de 30 segundos; validade de 60 segundos. A API adota janela de tolerância de 90 segundos para determinar estado offline.
- O consumidor n8n/API deve validar timestamps e avançar `ultima_comunicacao_em` apenas para instantes mais recentes, sem inventar persistência na ausência de captura.

## 4. Pausa de Monitoramento e Regras de Negócio no n8n

O produtor Python opera continuamente e de forma desacoplada, publicando detecções independentemente de existir sessão ativa ou pausa.

A **API Node.js é a autoridade de negócio**:
1. Para cada alerta recebido, a API verifica se existe exatamente uma sessão de monitoramento `ATIVO` vinculada à câmera.
2. **Sessão Pausada / Inexistente**: se a API retornar erro HTTP `409` com código `ACTIVE_MONITORING_NOT_FOUND`, o n8n deve tratar como **recusa de negócio normal**:
   - A API não cria alertas nem dispara notificações FCM aos usuários.
   - **Orientação crítica para o n8n**: O nó de requisição HTTP **NÃO DEVE** retentar esse erro automaticamente ou reencaminhar a mensagem para a fila aguardando uma suposta retomada. O n8n deve confirmar a mensagem (ACK) ou arquivar o log operacional. Essa regra é estrita para `ACTIVE_MONITORING_NOT_FOUND` e não deve ser generalizada para outros erros 409.
3. Não há garantia nem promessa de repositório histórico no Python para eventos recusados durante a pausa.
4. Alertas aceitos antes de uma pausa concluem seu ciclo de entrega normalmente.

## 5. Expiração e Política de TTL

- **Padrão**: `ALERT_TTL_S=0` (sem expiração automática no produtor; mensagens são publicadas sem a propriedade AMQP `expiration`).
- O RabbitMQ não deve receber `"0"` como string de expiração.
- Se configurado com valor positivo (`ALERT_TTL_S > 0`), mensagens vencidas na outbox local antes da publicação serão descartadas com aviso em log.

## 6. Isolamento de Testes

- `TEST_MODE=true` e `PUBLISH_RABBIT=false` são os padrões de segurança.
- O campo `technicalData.test` é informativo e não impede processamento pela API.
- Para publicar no RabbitMQ em modo de teste, é mandatória a liberação explícita `ALLOW_TEST_PUBLISH=true`. Sem ela, o serviço falha imediatamente na inicialização.
- Ambientes de teste devem utilizar câmeras de teste com `CODIGO_INTEGRACAO` específico e workflows n8n com destinatários controlados.
