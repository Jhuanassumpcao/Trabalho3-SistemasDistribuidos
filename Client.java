public class Client {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java Client <ip> <port>");
            return;
        }

        String ip = args[0];
        int port = Integer.parseInt(args[1]);

        try {
            StableMulticast stableMulticast = new StableMulticast(ip, port, new IStableMulticast() {
                @Override
                public void deliver(String msg) {
                    System.out.println("Mensagem entregue: " + msg);
                }
            });

            stableMulticast.msend();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
