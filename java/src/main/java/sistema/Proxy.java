package sistema;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/** Pub-sub proxy: publishers connect to 5557 and subscribers to 5558. */
final class Proxy {
    private Proxy() {}

    static void run() {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket publishers = context.createSocket(ZMQ.XSUB);
            publishers.bind("tcp://*:5557");
            ZMQ.Socket subscribers = context.createSocket(ZMQ.XPUB);
            subscribers.bind("tcp://*:5558");
            System.out.println("proxy: XSUB 5557 / XPUB 5558");

            ZMQ.proxy(publishers, subscribers, null);
        }
    }
}
