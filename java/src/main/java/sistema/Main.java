package sistema;

/** Entry point that selects the client or server process. */
public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("uso: java -jar app.jar server|client");
        }

        switch (args[0]) {
            case "server" -> Server.run();
            case "client" -> Client.run();
            case "reference" -> ReferenceService.run();
            default -> throw new IllegalArgumentException("modo desconhecido: " + args[0]);
        }
    }
}
