# Contrato proposto Python -> RabbitMQ -> n8n

**Proposta v1, configurável.** Nenhum endpoint, campo Prisma ou fluxo do aplicativo é criado por este pacote. O colega deve mapear este payload ao contrato real do domínio. Identificadores internos da câmera não devem ser inferidos pelo produtor; CODIGO_INTEGRACAO deve corresponder ao cadastro oficial. camera_id é identificação operacional do produtor; codigo_integracao é a chave para resolver a câmera no domínio.

## Topologia

| Recurso | Padrão da Stack de teste |
|---|---|
| Exchange durável, tipo Topic | poolsafe.teste.eventos.v1 |
| Routing key de alertas | poolsafe.alerta |
| Routing key de heartbeat | poolsafe.heartbeat |
| Fila durável de alertas | poolsafe.teste.alertas.v1 |
| Fila durável de heartbeat | poolsafe.teste.heartbeat.v1 |

A exchange encaminha por routing key; o n8n consome as filas. Nome da fila não é routing key automaticamente. O padrão de heartbeat inclui TTL de fila 60000 ms. Nomes exclusivos de teste evitam alterar recursos já em uso.

## Alerta ilustrativo

Os campos de hash/ID abaixo são exemplos, não resultados medidos.

```json
{
  "schema_version": 1,
  "event_id": "UUID-estavel-nas-tentativas",
  "tipo": "alerta",
  "camera_id": "camera_teste_01",
  "codigo_integracao": "codigo-cadastrado-no-dominio",
  "observado_em": "2026-10-19T01:00:00.000+00:00",
  "confirmado_em": "2026-10-19T01:00:00.100+00:00",
  "expira_em": "2026-10-19T01:02:00.000+00:00",
  "teste": true,
  "origem": "video",
  "run_id": "UUID-da-execucao",
  "segmento": 0,
  "frame_processado": 150,
  "video_time_s": 4.9667,
  "modelo": {"versao": "v6", "sha256": "HASH_REAL_DO_ARQUIVO"},
  "deteccao": {
    "track_id": 7,
    "classe": "pessoa_queda",
    "confianca": 0.82,
    "bbox_xyxy": [100, 80, 300, 400]
  },
  "regra": {
    "frames_consecutivos": 8,
    "conf_minima": 0.25,
    "frames_normais_encerrar": 15,
    "cooldown_s": 3,
    "max_intervalo_s": 0.5
  },
  "evidencia": null
}
```

bbox_xyxy usa pixels do frame original; confiança da caixa não é acurácia. O frame de confirmação é distinto do início do evento. Em stream, video_time_s é null. observado_em é tempo UTC de decodificação, não timestamp garantido do sensor. Em vídeo, é tempo de reprodução e o campo teste é obrigatoriamente true. segmento/run_id delimitam a validade do track_id; IDs não são globais ou cadastros de pessoas.

Quando habilitada, evidencia contém `mime_type: image/jpeg` e `jpeg_base64`. É um JPEG sem caixas, reduzido a no máximo 640 pixels na maior dimensão, até 250000 bytes antes de base64. Falha de codificação ou tamanho excedido deixa evidencia=null. Não existe URL pública. O consumidor deve impor limites e armazenamento privado, com acesso exclusivamente pela API autenticada do aplicativo. A imagem fica temporariamente no transporte/outbox até confirmação de publicação; este pacote não cria armazenamento definitivo de evidências no domínio.

## Heartbeat ilustrativo

```json
{
  "schema_version": 1,
  "event_id": "UUID-do-heartbeat",
  "tipo": "heartbeat",
  "camera_id": "camera_teste_01",
  "codigo_integracao": "codigo-cadastrado-no-dominio",
  "observado_em": "2026-10-19T01:00:00.000+00:00",
  "expira_em": "2026-10-19T01:01:00.000+00:00",
  "teste": true
}
```

Não significa que o detector ou app estejam funcionando. Nunca emitir a partir de arquivo para atualizar uma câmera real como online. No consumidor, heartbeats de teste devem ser isolados dos cadastros operacionais.

## Obrigações do consumidor

1. Validar JSON, versão, tipo, tamanho, campos, datas e a associação oficial da câmera. Não confiar em IDs enviados sem resolver cadastro/autorização no domínio.
2. Tratar `teste=true` em fluxo isolado, com destinatário de teste explícito. Não notificar usuários reais acidentalmente.
3. Comparar expira_em com relógio sincronizado; mensagens vencidas não devem gerar aviso novo. TTL do RabbitMQ não valida sozinho o instante do evento.
4. Para heartbeat, rejeitar instantes inválidos, antigos ou futuros fora da tolerância definida pela equipe. Avançar ultima_comunicacao_em só se observado_em for maior. Não gravar ONLINE/OFFLINE: API deriva conectividade.
5. Para alerta, deduplicar event_id de forma durável e transacional. Retransmissão do mesmo ID não deve criar um segundo alerta/notificação. `track_id` NÃO é chave de idempotência.
6. Aplicar estado da sessão/pausa conforme API/banco canônicos. Python continua publicando. Durante pausa, registrar de acordo com o domínio e não enviar notificação. Para eventos atrasados, definir explicitamente se a decisão usa o estado na observação ou no consumo; este produtor não possui monitoramento_id nem histórico de sessão.
7. Persistir alerta/evidência/destinatários conforme migrations e contratos reais. Não improvisar SQL ou tabelas a partir deste exemplo.
8. Definir entrega de notificações idempotente: persistir intenção/estado de entrega para retries, sem marcar um evento como inteiramente concluído antes de tratar as etapas necessárias. Falha no FCM precisa ser retomável sem duplicar o alerta.
9. ACK somente depois do marco durável definido pelo workflow. Se o nó n8n usado confirmar automaticamente antes do processamento, documentar a limitação ou usar um consumidor que exponha ACK/retry adequados. Não há garantia de consumo implementada por este pacote.
10. Definir retries limitados e DLQ para falhas permanentes; evitar requeue infinito de payload inválido. Definir retenção e expurgo de eventos/imagens.

O produtor usa publisher confirms e mandatory, preserva pendentes no SQLite e reenvia com mesmo event_id. Pode duplicar após aceitação pelo broker se cair antes de remover da outbox. Confirm do broker não é recebimento pelo n8n nem pelo aplicativo. Eventos com IDs novos, mesmo que semelhantes, são episódios distintos para o transporte: eventual deduplicação semântica precisa de regra própria e avaliação.

## Validade e expiração

Alertas têm validade operacional proposta de 120 segundos desde a observação do frame de confirmação. Antes de publicar, o worker calcula TTL remanescente. Expirados locais são removidos com aviso no log, e não arquivados como histórico completo. Heartbeats expiram em 60 segundos; somente o mais recente fica pendente na memória. Ajustar as janelas à política do domínio antes de operação definitiva.

## Meta de oito segundos

Registrar início humano anotado, frame de confirmação, publicação, consumo, persistência e recebimento no dispositivo. Sincronizar relógios. O pacote fornece observado_em/confirmado_em, mas não mede sozinho início de queda, atraso do sensor ou entrega. Medir latência só dos acertos sem informar eventos perdidos produz avaliação incompleta.
