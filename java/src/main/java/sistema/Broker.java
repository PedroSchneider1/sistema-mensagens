package sistema;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/** Request-reply broker: clients connect to 5555 and servers to 5556, in round-robin. */
final class Broker {
    private Broker() {}

    static void run() {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket clients = context.createSocket(ZMQ.ROUTER);
            clients.bind("tcp://*:5555");
            ZMQ.Socket servers = context.createSocket(ZMQ.DEALER);
            servers.bind("tcp://*:5556");
            System.out.println("broker: ROUTER 5555 / DEALER 5556");

            // Encaminha nos dois sentidos; o DEALER distribui as requisições em round-robin
            ZMQ.proxy(clients, servers, null);
        }
    }
}
