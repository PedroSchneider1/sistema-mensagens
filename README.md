# Sistema de troca de mensagens

Sistema distribuído em que bots (clientes) criam canais, publicam mensagens e recebem as mensagens dos canais em que estão inscritos. Várias réplicas de servidor atendem os clientes e todas guardam todos os dados.

## Visão geral

```
                    REQ/REP                          REQ/REP
 Cliente ───────────────────────► Broker ───────────────────────► Servidor 1..3
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

**Servidor.** Guarda canais e mensagens, responde às requisições dos clientes e publica cada mensagem no canal. Existem três, e cada um tem o seu próprio banco SQLite. Os servidores também conversam entre si para replicar dados, sincronizar relógios e eleger um coordenador.

**Broker.** Fica entre os clientes e os servidores. O cliente conhece apenas o broker, e não sabe quantos servidores existem nem quais estão no ar. Isso permite adicionar ou remover servidores sem mexer nos clientes.

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

O broker entrega as requisições aos servidores em rodízio: a 1ª vai para o servidor 1, a 2ª para o 2, a 3ª para o 3, a 4ª volta ao 1, e assim por diante. É simples e distribui a carga por igual. Como consequência, **cada requisição de um mesmo cliente pode cair em um servidor diferente**, o que causa o problema tratado na próxima seção.

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
| Python 3.13 | Todos os componentes |
| [ZeroMQ](https://zeromq.org/) (`pyzmq`) | Comunicação: `REQ/REP`, `ROUTER/DEALER` (broker), `XSUB/XPUB` (proxy), `PUB/SUB` |
| Protocol Buffers (`protobuf`) | Formato binário das mensagens, definido em `src/message.proto` |
| SQLite | Banco de cada servidor (arquivo único, sem processo separado) |
| Docker Compose | Sobe todos os componentes |

## Como executar

Pré-requisito: Docker com o Compose.

```bash
cd container
docker compose up --build
```

Isso inicia o broker, o proxy, a referência, três servidores e dois clientes (`client_pedro` e `client_gabriel`). Os dados de cada servidor ficam em volumes (`data_1`, `data_2`, `data_3`) e sobrevivem a reinícios.

Comandos úteis:

```bash
docker compose logs -f server_1 client_pedro   # acompanhar os logs
docker compose stop server_2                   # derrubar um servidor
docker compose start server_2                  # religar (ele recupera o que perdeu)
docker compose down -v                         # parar e apagar os dados
```

Para inspecionar um banco:

```bash
docker compose exec server_1 python -c "
import sqlite3
db = sqlite3.connect('/data/server_1.db')
print(db.execute('SELECT origin, MAX(seq) FROM entries GROUP BY origin').fetchall())"
```

Se o `message.proto` for alterado, regenere o código com
`python -m grpc_tools.protoc -I src --python_out=src src/message.proto`
(requer `grpcio-tools`).

## Limitações conhecidas

- **Eleição concorrente:** dois servidores podem se declarar coordenadores ao mesmo tempo. O sistema se corrige no anúncio seguinte.
- **Servidor bloqueado:** enquanto um servidor espera (réplica, par ou referência), ele não atende outros clientes, por até 3 s.
- **Vetor global:** o vetor cobre todos os dados, mais do que o necessário, e pode fazer o cliente esperar por entradas que ele não usa.
- **Canais com nomes de tópico:** um canal chamado `servers`, `replica` ou `eventlog` colidiria com os tópicos internos (o ZeroMQ casa tópicos por prefixo).
- **Canais criados ao mesmo tempo:** dois servidores que criam o mesmo canal ao mesmo tempo acabam com um só (o nome é unido na replicação).
- **Perda total:** se todos os servidores caírem e um deles perder o volume, ele reinicia o `seq` do zero e os outros descartam as suas novas entradas como duplicadas.
