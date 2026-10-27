# Registro de validação — 27/10/2026

Foram executados 61 testes `unittest` locais, aprovados. Um deles executa os 47 cenários originais do script de validação V4. Os demais incluem transporte e fixture histórica, 15 verificações do adaptador/pipeline e dez verificações de fronteira, falhas, logs, recursos, saúde e proveniência. A comparação determinística cobre dez sequências de 500 frames; não são novos vídeos ou participantes.

Também foram verificados sintaxe/bytecode Python e parsing estrutural YAML de `compose.yaml`, `compose.gpu.yaml` e workflow GitHub. O parsing YAML não substitui `docker compose config` nem uma implantação real.

Não executado: build Docker, downloads das dependências na imagem, inferência com pesos reais, CUDA, câmera real, rede WARP do servidor, RabbitMQ real, API/n8n e FCM. Os testes de pipeline simulam essas fronteiras. Não há benchmark da VPS nem evidência de latência até o aplicativo nesta entrega.

O teste de AST comprova preservação das definições das classes V4 extraídas. As verificações da fachada não duplicam o contador temporal: conferem formato de entrada e consistência estrutural de resposta. A política de stream mantém o comportamento da referência; não introduz reset temporal independente.

Nenhuma mensagem externa foi enviada, nenhum serviço foi implantado e os arquivos anexados originais foram preservados. O pacote é uma nova cópia para revisão e homologação.
