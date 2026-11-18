package sistema;

/** Entry point that selects which process to run. */
public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("uso: java -jar app.jar broker|proxy|reference|server|client");
        }

        switch (args[0]) {
            case "broker" -> Broker.run();
            case "proxy" -> Proxy.run();
            case "server" -> Server.run();
            case "client" -> Client.run();
            case "reference" -> ReferenceService.run();
            default -> throw new IllegalArgumentException("modo desconhecido: " + args[0]);
        }
    }
}
