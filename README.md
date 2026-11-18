# Sistema de troca de mensagens

Sistema distribuído em que bots (clientes) criam canais, publicam mensagens e recebem as mensagens dos canais em que estão inscritos. Várias réplicas de servidor atendem os clientes e todas guardam todos os dados.

O projeto tem **duas implementações completas, em Python e em Java**, que conversam entre si: 3 servidores e 2 clientes em cada linguagem, totalizando **6 servidores e 4 clientes** trocando mensagens ao mesmo tempo. Um cliente Java usa um servidor Python e vice-versa, e os servidores das duas linguagens se replicam, elegem um coordenador e sincronizam os relógios juntos. Isso é possível porque a comunicação segue um contrato único (`proto/message.proto`), e não depende da linguagem.

## Estrutura

```
proto/message.proto     contrato das mensagens, usado pelas duas linguagens
python/                 broker, proxy, referência, servidor, cliente (Python 3.13)
java/                   broker, proxy, referência, servidor, cliente (Java 17, Maven)
container/              Dockerfiles e docker-compose.yml do sistema misto
```

## Visão geral

```
                    REQ/REP                          REQ/REP
 Cliente ───────────────────────► Broker ───────────────────────► Servidores 1..6
    ▲        (5555)            (round-robin)        (5556)          │   │   ▲
    │                                                               │   │   │ REQ/REP
    │ SUB (5558)                                                    │   │   │ (5560)
    │                                                  PUB (5557)   ▼   │   ▼
    └──────────────────────────────────────────────────────────── Proxy  │  Servidores
                                                         ▲  (XSUB/XPUB)  │  (pares)
                                                         └───────────────┘
                                                      SUB (5558): replicação e anúncios

 Servidores ──► Referência (5559): rank, lista de servidores e heartbeat
```

## Componentes

**Cliente (bot).** Faz login, cria e lista canais, publica mensagens e lê o histórico. Também se inscreve em alguns canais para receber o que os outros publicam. Mantém o estado da sua sessão (relógio lógico e vetor de versões, explicados abaixo).

**Servidor.** Guarda canais e mensagens, responde às requisições dos clientes e publica cada mensagem no canal. Existem seis (3 em Python e 3 em Java), e cada um tem o seu próprio banco SQLite. Os servidores também conversam entre si para replicar dados, sincronizar relógios e eleger um coordenador.

**Broker.** Único e compartilhado pelas duas linguagens. Fica entre os clientes e os servidores. O cliente conhece apenas o broker, e não sabe quantos servidores existem nem quais estão no ar. Isso permite adicionar ou remover servidores sem mexer nos clientes.

**Proxy.** Faz o mesmo papel do broker para o modelo publicador/assinante (pub/sub). Os servidores publicam no proxy e os clientes e os outros servidores assinam nele, por tópico. Assim ninguém precisa conhecer quem publica ou quem assina.

**Serviço de referência.** Atribui um *rank* a cada servidor, mantém a lista dos servidores ativos com os seus endereços e remove da lista quem para de enviar *heartbeats* (sinais periódicos de "estou vivo").

### Broker × proxy

| | Broker | Proxy |
|---|---|---|
| Modelo | Requisição/resposta | Publicador/assinante |
| Quem recebe | **Um** servidor, escolhido a cada requisição | **Todos** os assinantes do tópico |
| Resposta | Volta ao cliente que perguntou | Não há resposta |
| Uso aqui | Criar canal, listar, publicar, histórico | Entregar mensagens, replicar dados, anunciar o coordenador |

### Balanceamento round-robin

O broker entrega as requisições aos servidores em rodízio: a 1ª vai para o servidor 1, a 2ª para o 2, a 3ª para o 3, e assim por diante até o 6º, quando volta ao 1. Como o broker só encaminha bytes, o servidor que atende pode ser Python ou Java, independentemente da linguagem do cliente. É simples e distribui a carga por igual. Como consequência, **cada requisição de um mesmo cliente pode cair em um servidor diferente**, o que causa o problema tratado na próxima seção.

## Replicação e consistência

### Por que replicar

Com o round-robin, cada servidor receberia só uma parte das mensagens. Se um servidor caísse, parte do histórico seria perdida, e quem pedisse o histórico a um servidor veria só um pedaço dele. Por isso **todos os servidores guardam todos os dados**.

Antes havia um único PostgreSQL compartilhado, o que resolvia o histórico, mas criava um ponto único de falha. Agora cada servidor tem o seu SQLite e os servidores se mantêm iguais entre si, sem banco central.

### Como os dados são replicados

É uma **replicação ativa** (multi-master): qualquer servidor aceita escritas, grava localmente e publica a alteração no tópico `replica` do proxy. Os outros a aplicam no seu banco.

- Cada alteração tem um identificador `(servidor de origem, seq)`, com `seq` crescente e sem buracos por origem. Isso torna a aplicação **idempotente**: repetir uma entrada não tem efeito.
- O histórico de um canal é ordenado por `(relógio lógico, origem, seq)`, então **todos os servidores mostram a mesma ordem**.
- Também se replicam o estado (canais e publicações) e o registro bruto das mensagens trocadas (tópico `eventlog`). O estado é a parte que importa para a consistência. O registro é replicado de forma eventual.

### Reparo

O pub/sub não garante entrega, e servidores podem cair. Por isso cada servidor também **busca o que lhe falta** nos outros: ao iniciar, quando percebe uma lacuna no `seq` e periodicamente. Um servidor que volta com o volume vazio recupera até as suas próprias entradas.

### Consistência centrada no cliente

Como cada requisição pode ir a um servidor diferente, e a replicação leva algum tempo, um cliente poderia criar um canal e logo depois não vê-lo na listagem, ou ver o histórico "encolher". Para evitar isso, o sistema usa **consistência centrada no cliente**, com estas garantias:

| Garantia | Significado |
|---|---|
| Leituras monotônicas | Uma leitura nunca devolve algo mais antigo do que o que o cliente já leu |
| Escritas monotônicas | As escritas de um cliente são aplicadas na ordem em que ele as fez |
| Leitura do que escreveu | O cliente sempre vê as suas próprias escritas |
| Escritas seguem leituras | Uma escrita só vale sobre dados pelo menos tão novos quanto os que o cliente leu |

Funciona assim:

1. O cliente mantém um **vetor de versões**: para cada servidor, o maior `seq` que ele já leu ou escreveu.
2. Envia o vetor em toda requisição e incorpora o vetor devolvido na resposta.
3. O servidor só responde depois de ter aplicado tudo o que o vetor indica. Se estiver atrasado, **espera a replicação chegar** (até 3 s). Se não chegar, responde **503** e o cliente tenta de novo, normalmente em outro servidor graças ao round-robin.

## Relógios e coordenador

- **Relógio lógico (Lamport):** cada processo incrementa um contador ao enviar e, ao receber, assume o maior valor visto. Ele ordena os eventos sem depender da hora real.
- **Sincronização do relógio físico (Cristian):** os servidores acertam o relógio com o do coordenador a cada 15 mensagens, compensando metade do tempo de ida e volta.
- **Eleição (bully):** o coordenador é o servidor de maior *rank* disponível. Se ele cai, o próximo servidor percebe e inicia uma eleição. O resultado é anunciado no tópico `servers`.

## Tecnologias

| Tecnologia | Para quê |
|---|---|
| Python 3.13 e Java 17 | As duas implementações (broker, proxy, referência, servidor e cliente em cada uma) |
| [ZeroMQ](https://zeromq.org/) (`pyzmq` e `jeromq`) | Comunicação: `REQ/REP`, `ROUTER/DEALER` (broker), `XSUB/XPUB` (proxy), `PUB/SUB` |
| Protocol Buffers | Formato binário das mensagens, definido em `proto/message.proto`. É o que permite a interoperação |
| SQLite | Banco de cada servidor (arquivo único, sem processo separado) |
| Docker Compose | Sobe todos os componentes |

## Como executar

Pré-requisito: Docker com o Compose.

```bash
cd container
docker compose up --build
```

Isso inicia, de uma vez:

| Componente | Python | Java |
|---|---|---|
| Servidores | `server_py_1..3` | `server_java_1..3` |
| Clientes | `client_py_pedro` (`pedro_client`), `client_py_gabriel` (`gabriel_client`) | `client_java_pedro` (`pedro_java`), `client_java_gabriel` (`gabriel_java`) |
| Compartilhados | `broker`, `proxy` e `reference` (implementação Python, usados pelos dois lados) | |

Os dados de cada servidor ficam em volumes (`data_py_N`, `data_java_N`) e sobrevivem a reinícios.

Comandos úteis:

```bash
docker compose logs -f server_py_1 server_java_1 client_py_pedro client_java_pedro   # acompanhar
docker compose stop server_java_2     # derrubar um servidor
docker compose start server_java_2    # religar (ele recupera o que perdeu)
docker compose down -v                # parar e apagar os dados
```

Para inspecionar o banco de um servidor Python:

```bash
docker compose exec server_py_1 python -c "
import sqlite3
db = sqlite3.connect('/data/server_py_1.db')
print(db.execute('SELECT origin, MAX(seq) FROM entries GROUP BY origin').fetchall())"
```

Os seis bancos devem ter o mesmo conteúdo. Para um servidor Java, copie o arquivo (`docker compose cp server_java_1:/data/server_java_1.db .`) e abra com `sqlite3`.

Se o `proto/message.proto` for alterado:
- **Python:** `python -m grpc_tools.protoc -I proto --python_out=python proto/message.proto` (requer `grpcio-tools`);
- **Java:** as classes são geradas pelo Maven na compilação.

## Limitações conhecidas

- **Eleição concorrente:** dois servidores podem se declarar coordenadores ao mesmo tempo. O sistema se corrige no anúncio seguinte.
- **Servidor bloqueado:** enquanto um servidor espera (réplica, par ou referência), ele não atende outros clientes, por até 3 s.
- **Vetor global:** o vetor cobre todos os dados, mais do que o necessário, e pode fazer o cliente esperar por entradas que ele não usa.
- **Canais com nomes de tópico:** um canal chamado `servers`, `replica` ou `eventlog` colidiria com os tópicos internos (o ZeroMQ casa tópicos por prefixo).
- **Canais criados ao mesmo tempo:** dois servidores que criam o mesmo canal ao mesmo tempo acabam com um só (o nome é unido na replicação).
- **Serviços compartilhados:** há um só broker, um só proxy e uma só referência. Se um deles cair, o sistema para de funcionar: sem o broker os clientes não chegam aos servidores, sem o proxy não há entrega de mensagens nem replicação e sem a referência os servidores não se encontram.
- **Usuários fixos:** os usuários permitidos (`pedro_client`, `gabriel_client`, `pedro_java`, `gabriel_java`) estão escritos no código dos servidores, nas duas linguagens.
- **Coordenador:** o *rank* depende da ordem em que os servidores se registram, então o coordenador pode ser Python ou Java.
- **Perda total:** se todos os servidores caírem e um deles perder o volume, ele reinicia o `seq` do zero e os outros descartam as suas novas entradas como duplicadas.
