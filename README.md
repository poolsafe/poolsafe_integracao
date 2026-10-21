# PoolSafe — Produtor Python de Integração (Worker de Visão Computacional)

Este pacote executa o worker de visão computacional da solução PoolSafe: captura de vídeo/câmera -> inferência YOLO11 -> rastreamento ByteTrack -> confirmação temporal de queda -> outbox SQLite local -> mensageria RabbitMQ.

O n8n, a API Node.js, o banco MySQL, as notificações push FCM e o aplicativo móvel são serviços externos. O worker não administra filas, usuários, autorizações ou regras de negócio da API.

---

## 1. Fluxo de Entrega e CI/CD

O repositório adota o mesmo padrão da API PoolSafe:

```text
Git push (main) 
  -> GitHub Actions valida (Python 3.11, compilação, testes unitários, docker compose config)
  -> Constrói imagem Docker (linux/amd64)
  -> Executa verificação de importação das bibliotecas (torch, torchvision, cv2, ultralytics, pika)
  -> Publica imagem no GitHub Container Registry (GHCR) privado
  -> Implantação manual controlada via Portainer (Stack Git ou Compose)
```

### Imagem e Tags Publicadas

- **Referência Canônica da Imagem:** `ghcr.io/poolsafe/poolsafe_integracao` *(o namespace do GHCR deve ser sempre em letras minúsculas)*.
- **Tags Geradas:**
  - `main`: reflete o commit mais recente integrado na branch principal;
  - `sha-<SHA_40_DIGITOS>`: tag imutável do commit exato (ex.: `sha-c1a2b3...`).
- **Digest e Rastreabilidade:** O digest exato da imagem publicada (`sha256:...`) e os metadados OCI (repositório e revisão) são registrados no **Summary** da execução do GitHub Actions (`$GITHUB_STEP_SUMMARY`) e nos detalhes do pacote no GitHub Packages.

---

## 2. Separação Estrita: Código e Modelo (Pesos Independentes)

A imagem Docker empacota exclusivamente o código Python e suas dependências de runtime. Os pesos do modelo (`best.pt`), os vídeos de ensaio e os dados de persistência **ficam fora da imagem**, montados a partir do host da VPS.

### Estrutura de Pastas Recomendada no Host (VPS)

```text
/opt/poolsafe/
├── modelos/
│   └── releases/
│       ├── modelo-001/
│       │   └── best.pt          # Pesos da versão 001
│       └── modelo-002/
│           └── best.pt          # Nova versão de pesos
├── entrada/
│   └── teste.mp4                # Vídeo de ensaio local
└── dados/
    ├── ensaio-modelo-001/       # Persistência isolada do ensaio 001
    └── ensaio-modelo-002/       # Persistência isolada do ensaio 002
```

### Regras de Governança dos Pesos

1. **Pastas distintas por versão:** Cada release de pesos deve residir em sua própria pasta (`/opt/poolsafe/modelos/releases/<versao>/best.pt`).
2. **Imutabilidade em execução:** Nunca sobrescreva o arquivo `best.pt` enquanto o container estiver rodando.
3. **Identificação por SHA-256:** Na inicialização, o worker calcula e registra nos logs o SHA-256 exato do arquivo carregado e confere as classes esperadas (`pessoa_normal`, `pessoa_queda`).
4. **Sem necessidade de rebuild:** A troca de pesos compatíveis com as mesmas classes não exige rebuild da imagem Docker — basta alterar a variável `HOST_MODEL_DIR` e o `MODEL_VERSION` no Compose.
5. **Ensaios simultâneos:** Devem utilizar diretórios de dados (`HOST_DATA_DIR`) separados para evitar conflito de locks e concorrência no SQLite (`outbox.sqlite3`).
6. **Recriação obrigatória:** Um simples *restart* do container não aplica novas variáveis de ambiente ou montagens de volume; é necessário recriar o container (no Portainer: *Update the stack* com *Re-pull image* ou recriação do container).
7. **Rollback:** Para reverter um modelo ou versão de código com problemas, altere `WORKER_IMAGE`, `HOST_MODEL_DIR` e `MODEL_VERSION` de volta para a combinação estável anterior homologada.

---

## 3. Implantação no Portainer (Manual)

### 3.1. Requisitos Prévios no Portainer / Host

1. **Rede Docker Compartilhada:** Certifique-se de que a rede bridge externa `poolsafe-network` existe na VPS. Se necessário, crie-a com `docker network create poolsafe-network`.
2. **Permissões de Pastas no Host:**
   O container executa como usuário não privilegiado `UID:GID 10001`. A pasta de dados gravável precisa ter a titularidade correta:
   ```bash
   sudo mkdir -p /opt/poolsafe/modelos/releases/modelo-001 /opt/poolsafe/entrada /opt/poolsafe/dados/ensaio-modelo-001
   sudo chown -R 10001:10001 /opt/poolsafe/dados/ensaio-modelo-001
   # As pastas de modelo e entrada são montadas como somente leitura (:ro)
   sudo chmod -R 755 /opt/poolsafe/modelos /opt/poolsafe/entrada
   ```

### 3.2. Acesso ao GHCR Privado vs. Acesso ao Repositório Git

> [!IMPORTANT]
> **Acesso ao repositório Git e acesso ao registry Docker são responsabilidades distintas no Portainer:**
> - **Git Repository Credential:** Usada pelo Portainer para ler o `compose.yaml` do repositório Git (Personal Access Token ou Deploy Key com permissão de leitura de código `repo` / `contents:read`).
> - **Registry Credential:** Usada pelo Docker Engine para puxar (`docker pull`) a imagem do GHCR. No Portainer, configure em **Settings -> Registries -> Add registry -> Custom**:
>   - **Name:** `GHCR PoolSafe`
>   - **Registry URL:** `ghcr.io`
>   - **Authentication:** ativada, com seu usuário do GitHub e um PAT (Personal Access Token) contendo o escopo `read:packages`.
> 
> *Nota sobre visibilidade e permissões:* Após o primeiro push da imagem pelo GitHub Actions, acesse **GitHub -> Perfil/Organização -> Packages -> poolsafe_integracao** para verificar a visibilidade (privada) e conceder acesso de leitura (`Read`) ao token ou equipe responsável.

### 3.3. Configuração da Stack no Portainer

1. Acesse o Portainer -> **Stacks** -> **Add stack**.
2. Selecione o método de implantação preferido:
   - **Repository (Recomendado):**
     - **Repository URL:** `https://github.com/poolsafe/poolsafe_integracao.git`
     - **Repository reference:** `refs/heads/main`
     - **Compose path:** `compose.yaml`
     - **Authentication:** Habilite e preencha as credenciais Git.
   - **Web editor:** Cole diretamente o conteúdo de [compose.yaml](file:///c:/Users/Daniel/Documents/Github-Poolsafe-TCC/poolsafe_integracao/compose.yaml).
3. Na seção **Environment variables**, configure as variáveis baseando-se no arquivo [.env.example](file:///c:/Users/Daniel/Documents/Github-Poolsafe-TCC/poolsafe_integracao/.env.example).
4. Para o **primeiro ensaio**, defina a imagem com tag por commit:
   ```env
   WORKER_IMAGE=ghcr.io/poolsafe/poolsafe_integracao:sha-<SHA_DO_COMMIT>
   ```

---

## 4. Primeiro Ensaio com Vídeo (Sem Publicação)

O primeiro ensaio deve validar o pipeline de visão computacional em ambiente real sem emitir mensagens ao RabbitMQ nem acionar alertas ao aplicativo móvel.

### Configuração do Ensaio Seguro

Defina as variáveis na Stack do Portainer:

```env
WORKER_IMAGE=ghcr.io/poolsafe/poolsafe_integracao:main
CAMERA_ID=camera_teste_01
SOURCE_MODE=video
SOURCE=/entrada/teste.mp4
HOST_MODEL_DIR=/opt/poolsafe/modelos/releases/modelo-001
HOST_VIDEO_DIR=/opt/poolsafe/entrada
HOST_DATA_DIR=/opt/poolsafe/dados/ensaio-modelo-001
MODEL_VERSION=modelo-001
DEVICE=cpu
TEST_MODE=true
ALLOW_TEST_PUBLISH=false
PUBLISH_RABBIT=false
INCLUDE_EVIDENCE=false
```

### O que Conferir nos Logs do Container

Acesse **Containers -> poolsafe-worker -> Logs** e verifique:

1. **Carregamento do modelo:** Verifique a emissão da linha de log contendo:
   - `versão=<MODEL_VERSION>`
   - `sha256=<HASH_SHA256>`
   - `classes={0: 'pessoa_normal', 1: 'pessoa_queda'}`
   - `device=cpu`
2. **Modo e parâmetros operacionais:**
   - `Modo=video teste=True publicação=False; frames=8 cooldown=3.0s`
3. **Ausência de publicação e heartbeat:**
   - Em `SOURCE_MODE=video`, o heartbeat de câmera fica completamente desativado.
   - Nenhuma conexão AMQP é aberta quando `PUBLISH_RABBIT=false`.
4. **Detecções e confirmações:**
   - Eventos locais registrados no log e detalhados em `/dados/deteccoes.jsonl`.
   - Tempo de processamento por frame e consumo de CPU/memória no container.
   - Encerramento limpo ao atingir o fim do arquivo de vídeo.

> [!NOTE]
> Este worker não gera arquivos MP4 com caixas anotadas nem salva evidências gráficas no ensaio local se a publicação estiver desligada. Toda auditoria de inferência no ensaio é realizada via logs e pelo arquivo JSONL de detecções.

---

## 5. Habilitando Publicação no RabbitMQ e Câmera ao Vivo

### 5.1. Publicação de Teste no RabbitMQ

Após homologar o ensaio sem publicação:

```env
PUBLISH_RABBIT=true
ALLOW_TEST_PUBLISH=true
CODIGO_INTEGRACAO=codigo_da_camera_na_api
RABBIT_HOST=rabbitmq
RABBIT_PORT=5672
RABBIT_VHOST=poolsafe
RABBIT_USER=poolsafe_python
RABBIT_PASSWORD=senha_do_rabbit
RABBIT_EXCHANGE=poolsafe.events
```

### 5.2. Câmera ao Vivo (RTSP) e Heartbeat

```env
SOURCE_MODE=stream
SOURCE=rtsp://usuario:senha@ip_camera:554/stream1
CAMERA_ID=piscina_area_lazer
CODIGO_INTEGRACAO=codigo_cadastrado_na_api
```

- Em modo `stream`, o heartbeat de conectividade da câmera é emitido periodicamente (a cada ~30s com TTL de 60s) a partir da decodificação confirmada de frames reais.
- O restart na Stack pode ser alterado para `unless-stopped` após validação da estabilidade do fluxo RTSP.

---

## 6. Arquivos e Responsabilidades do Módulo

| Arquivo | Função |
|---|---|
| `main.py` | Carrega modelo, executa rastreamento YOLO/ByteTrack, gerencia ciclo de vida e encerramento |
| `config.py` | Validação estrita de tipos e regras das variáveis de ambiente |
| `captura.py` | Gerenciamento de conexão RTSP com reconexão contínua e desacoplamento de frames |
| `regras.py` | Motor de confirmação temporal de quedas (frames consecutivos, quebra por tempo, cooldown) |
| `transporte.py` | Fila local outbox em SQLite e publicação persistente (`publisher confirms`) no RabbitMQ |
| `Dockerfile` | Definição da imagem de runtime em Python 3.11 com usuário não-root (UID 10001) |
| `compose.yaml` | Stack única para implantação no Portainer/Docker Compose |
| `.env.example` | Template de configuração com valores seguros para ensaios |
| `CONTRATO_N8N.md` | Especificação dos payloads de risco e heartbeat para os consumidores |
| `tests/` | Suíte de testes unitários isolados (sem necessidade de GPU, câmera ou broker) |

---

## 7. Testes e Validação Local

Para executar a validação localmente:

```bash
# Validação da sintaxe e compilação dos scripts Python
python -m compileall -q .

# Execução da suíte de 21 testes unitários isolados
python -m unittest discover -s tests -v

# Validação do Compose (fornecendo imagem de teste)
WORKER_IMAGE=ghcr.io/poolsafe/poolsafe_integracao:ci-validate docker compose config
```
