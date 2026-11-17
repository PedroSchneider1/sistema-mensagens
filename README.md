# Sistema de troca de mensagens

Protótipo de um sistema de troca de mensagens entre bots. Nesta Parte 1, o cliente e o servidor são implementados em Java 17; o broker e o proxy continuam em Python. As requisições usam ZeroMQ e mensagens binárias Protocol Buffers, definidas em `java/src/main/proto/message.proto` (compatíveis com `src/message.proto`). Todas as mensagens carregam timestamp. O PostgreSQL persiste os logins aceitos com horário e os canais criados.

## Executar

Na raiz do repositório:

```sh
docker compose -f container/docker-compose.yml up --build
```

O compose inicia o broker, o proxy, o PostgreSQL, um servidor e os clientes `pedro_client` e `gabriel_client`. Cada cliente tenta fazer login, cria canais até existirem pelo menos cinco e imprime a lista recebida do servidor. Os usuários aceitos e nomes de canal permitidos estão definidos em `java/src/main/java/sistema/Main.java`; canais duplicados retornam status 409.

Para parar os serviços, use `Ctrl+C`. Para apagar também os dados persistidos durante os testes, execute `docker compose -f container/docker-compose.yml down -v`.

## Componentes

- `java/`: aplicação Java, geração das classes Protobuf via Maven, cliente e servidor.
- `src/broker.py` e `src/proxy.py`: encaminhamento de requisições e proxy de publicação ZeroMQ.
- `container/docker-compose.yml`: serviços de execução e banco de dados.

