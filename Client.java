import java.io.IOException;

public class Client implements IStableMulticast {
    private StableMulticast stableMulticast;

    public Client(String ip, Integer port) throws Exception {
        this.stableMulticast = new StableMulticast(ip, port, this);
    }

    @Override
    public void deliver(String msg) {
        System.out.println("Mensagem recebida: " + msg);
    }

    private void init() throws Exception {
        this.stableMulticast.run();
    }

    public static void main(String[] args) throws NumberFormatException, Exception {
        if (args.length != 2) {
            System.out.println(
                    "Uso: java StableMulticast <processIp> <processPort>");
            return;
        }

        try {
            Client client = new Client(args[0], Integer.parseInt(args[1]));
            client.init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
