# Sistema de troca de mensagens

Protótipo de um sistema de troca de mensagens entre bots. Broker, proxy, serviço de referência, servidores e clientes são implementados em Java 17. ZeroMQ faz a comunicação e Protocol Buffers define as mensagens em `java/src/main/proto/message.proto`, compatível com `src/message.proto` (contrato compartilhado com a versão Python, na branch `feat/python-version`).

## Executar

Na raiz do repositório:

```sh
docker compose -f container/docker-compose.yml up --build
```

O Compose inicia o broker Req-Rep, o proxy Pub-Sub, o serviço de referência, três servidores e dois clientes. Cada servidor tem um banco SQLite próprio em um volume persistente. Para acompanhar os servidores:

```sh
docker compose -f container/docker-compose.yml logs -f server_1 server_2 server_3
```

Para parar, use `Ctrl+C`. `docker compose -f container/docker-compose.yml down -v` também apaga os volumes e os dados.

## Replicação e consistência (parte 5)

O broker distribui cada requisição por round-robin, então uma mesma sessão pode chegar a servidores diferentes. Para que todos respondam com os mesmos dados, usamos **replicação ativa multi-master**: qualquer servidor pode aceitar uma escrita, gravá-la no seu SQLite e publicá-la no tópico `replica`. Os outros servidores aplicam a alteração localmente.

Cada alteração de estado (criação de canal ou publicação) recebe um identificador `(origem, sequência)`. A sequência cresce sem lacunas dentro de cada origem, o que permite ignorar duplicatas com segurança. O histórico é ordenado em todas as réplicas por `(relógio lógico, origem, sequência)`. O sistema também replica, pelo tópico `eventlog`, o registro serializado das requisições e respostas trocadas com os clientes.

Pub/Sub pode perder mensagens enquanto um servidor está indisponível. Por isso, além da replicação imediata, cada servidor busca as entradas que faltam nos pares: ao iniciar, quando detecta uma lacuna e periodicamente. Isso também permite que uma instância recupere as próprias entradas depois de reiniciar com um volume persistente.

Como a replicação é assíncrona, o cliente mantém um vetor de versões por sessão. Envia esse vetor em cada requisição e combina com o vetor devolvido pelo servidor. Antes de responder, o servidor espera até três segundos para ter aplicado o estado exigido pelo vetor. Se continuar atrasado, responde com status 503 e o cliente repete a requisição. Esse mecanismo preserva leituras monotônicas, escritas monotônicas, leitura do que foi escrito e escritas que seguem leituras, conforme a implementação Python.

Os servidores também mantêm relógios lógicos, elegem coordenador pelo algoritmo bully e sincronizam o relógio físico com o do coordenador (algoritmo de Cristian, compensando metade do tempo de ida e volta). O serviço de referência atribui ranks, lista servidores ativos e acompanha heartbeats.

## Componentes

- `java/`: broker, proxy, cliente, servidor, serviço e cliente de referência, armazenamento SQLite e entrada `Main` (`broker`, `proxy`, `reference`, `server` ou `client`); Maven gera as classes Protobuf.
- `src/message.proto`: definição das mensagens, compartilhada com a versão Python.
- `container/docker-compose.yml`: configuração para executar o sistema.
