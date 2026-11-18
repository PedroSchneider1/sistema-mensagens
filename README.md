# Sistema de troca de mensagens

Protótipo de um sistema de troca de mensagens entre bots. O cliente, os servidores e o serviço de referência são implementados em Java 17; o broker e os proxies continuam em Python. As mensagens usam ZeroMQ e Protocol Buffers, definidos em `java/src/main/proto/message.proto` (compatível com `src/message.proto`), e carregam timestamp e relógio lógico. O serviço de referência atribui ranks e mantém a lista de servidores ativos por heartbeat. Os servidores elegem um coordenador pelo algoritmo bully e sincronizam seus relógios pelo algoritmo de Berkeley. O PostgreSQL persiste logins aceitos, canais, mensagens trocadas e publicações.

## Executar

Na raiz do repositório:

```sh
docker compose -f container/docker-compose.yml up --build
```

O compose inicia o broker Req-Rep, o proxy Pub-Sub, o serviço de referência, o PostgreSQL, três servidores e os clientes `pedro_client` e `gabriel_client`. Cada cliente faz login, garante pelo menos cinco canais, assina três canais e publica mensagens aleatórias continuamente. As mensagens recebidas são exibidas com o canal, remetente, timestamps e relógio lógico. Cada servidor envia heartbeat a cada dez requisições recebidas e sincroniza o relógio a cada quinze. Os usuários aceitos estão definidos em `java/src/main/java/sistema/Server.java`; canais duplicados retornam status 409.

Os servidores persistem as requisições e respostas serializadas em Protobuf na tabela `messages`, e as publicações na tabela `publications`. O volume `pgdata` mantém esses dados entre reinicializações. O proxy Pub-Sub encaminha publicações pela porta 5557 (XSUB) e anúncios do coordenador no tópico `servers` para os servidores inscritos e publicações pela porta 5558 (XPUB) para os clientes.

Para parar os serviços, use `Ctrl+C`. Para apagar também os dados persistidos durante os testes, execute `docker compose -f container/docker-compose.yml down -v`.

## Componentes

- `java/`: aplicação Java separada em `Client`, `Server`, `ReferenceService`, `ReferenceClient`, `Database` e `TimeUtil`, além da entrada `Main`; as classes Protobuf são geradas via Maven.
- `src/broker.py` e `src/proxy.py`: encaminhamento de requisições e proxy de publicação ZeroMQ.
- `container/docker-compose.yml`: serviços de execução e banco de dados.

