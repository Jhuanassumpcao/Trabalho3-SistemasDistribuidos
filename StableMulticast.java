import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.ArrayList;
import java.util.List;

public class StableMulticast implements IStableMulticast {
    private int processId;
    private int[] vectorClock;
    private MulticastSocket socket;
    private InetAddress group;
    private int port;
    private AtomicBoolean running;
    private List<Integer> discoveredProcesses;
    private int[][] matrixClock; // Matriz de Relógios Vetoriais (MC)
    private List<String> buffer; // Lista de mensagens recebidas
    private Map<Integer, String> processAddresses; // Map de endereços dos processos
    private List<DelayedMessage> delayedMessages; // Lista de mensagens atrasadas

    @SuppressWarnings("deprecation")
    public StableMulticast(int processId, String groupAddress, int port, int numProcesses,
            Map<Integer, String> processAddresses) throws Exception {
        this.processId = processId;
        this.vectorClock = new int[numProcesses];
        this.matrixClock = new int[numProcesses][numProcesses]; // inicializa a matriz de relógios vetoriais
        this.socket = new MulticastSocket(port);
        this.group = InetAddress.getByName(groupAddress);
        this.socket.joinGroup(this.group);
        this.port = port;
        this.running = new AtomicBoolean(true);
        this.discoveredProcesses = new ArrayList<>();
        this.discoveredProcesses.add(processId);
        this.buffer = new ArrayList<>(); // Inicializa o buffer de mensagens
        this.processAddresses = processAddresses; // Inicializa o map de endereços
        this.delayedMessages = new ArrayList<>(); // Inicializa a lista de mensagens atrasadas
        System.out.println("Processo " + processId + " conectado ao grupo multicast " + groupAddress + ":" + port);
    }

    @Override
    public void deliver(String msg) {
        System.out.println("Mensagem recebida: " + msg);
    }

    // Método para enviar mensagem multicast
    public void msend(String msg, IStableMulticast sm) throws Exception {
        // atualiza o relógio vetorial e a matriz de relógios
        vectorClock[processId]++;
        matrixClock[processId][processId] = vectorClock[processId];

        // envia a mensagem multicast
        String messageWithClock = msg + "|" + Arrays.toString(vectorClock);
        byte[] buffer = messageWithClock.getBytes();
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, port);
        socket.send(packet);
        System.out.println("Mensagem enviada: " + msg);
        System.out.println("Buffer enviado: " + Arrays.toString(buffer));
        System.out.println("Relógio lógico ao enviar: " + Arrays.toString(vectorClock));
        System.out.println("Matriz de relógios ao enviar: " + Arrays.deepToString(matrixClock));
    }

    // Método para enviar mensagem unicast
    public void usend(String msg, int targetProcessId) throws Exception {
        // atualiza o relógio vetorial e a matriz de relógios
        vectorClock[processId]++;
        matrixClock[processId][processId] = vectorClock[processId];
        

        // envia a mensagem unicast
        String messageWithClock = msg + "|" + Arrays.toString(vectorClock);
        byte[] buffer = messageWithClock.getBytes();
        System.out.println("Enviando mensagem unicast para " + targetProcessId + ": " + msg);
        // printa todos os processos e seus endereços
        for (Map.Entry<Integer, String> entry : processAddresses.entrySet()) {
            System.out.println("Processo: " + entry.getKey() + " Endereço: " + entry.getValue());
        }
        System.out.println("Endereço do processo alvo: " + processAddresses.get(targetProcessId));

        InetAddress targetGroup = InetAddress.getByName(processAddresses.get(targetProcessId));
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, targetGroup, port);
        socket.send(packet);
        System.out.println("Mensagem unicast enviada para " + targetProcessId + ": " + msg);
        System.out.println("Buffer enviado: " + Arrays.toString(buffer));
        System.out.println("Relógio lógico ao enviar: " + Arrays.toString(vectorClock));
        System.out.println("Matriz de relógios ao enviar: " + Arrays.deepToString(matrixClock));
    }

    // Método para enviar as mensagens atrasadas
    public void sendDelayedMessages() throws Exception {
        // Ordena as mensagens atrasadas por relógio vetorial
        delayedMessages.sort((m1, m2) -> {
            for (int i = 0; i < m1.vectorClock.length; i++) {
                if (m1.vectorClock[i] != m2.vectorClock[i]) {
                    return Integer.compare(m1.vectorClock[i], m2.vectorClock[i]);
                }
            }
            return 0;
        });

        // Envia as mensagens atrasadas
        for (DelayedMessage delayedMessage : new ArrayList<>(delayedMessages)) {
            if (delayedMessage.isMulticast) {
                msend(delayedMessage.message, this);
            } else {
                usend(delayedMessage.message, delayedMessage.targetProcessId);
            }
            delayedMessages.remove(delayedMessage);
        }
    }

    // Método para descobrir instâncias
    public void discoverInstances() throws Exception {
        String discoveryMessage = "DISCOVER|" + processId;
        byte[] buffer = discoveryMessage.getBytes();
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, port);
        socket.send(packet);
        System.out.println("Enviada mensagem de descoberta");
    }

    // Método para anunciar que o processo está presente
    public void announcePresence() throws Exception {
        String announceMessage = "ANNOUNCE|" + processId;
        byte[] buffer = announceMessage.getBytes();
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, port);
        socket.send(packet);
        System.out.println("Anunciada presença do processo " + processId);
    }

    // Método para receber mensagens multicast
    public void receiveMulticast() throws Exception {
        byte[] packetBuffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(packetBuffer, packetBuffer.length);
        socket.setSoTimeout(1000);
        try {
            socket.receive(packet);
            String received = new String(packet.getData(), 0, packet.getLength());
            System.out.println("Pacote recebido: " + received);
            System.out.println(
                    "Buffer recebido: " + Arrays.toString(Arrays.copyOf(packet.getData(), packet.getLength())));

            String[] parts = received.split("\\|");
            if (parts.length != 2) {
                System.out.println("Formato de mensagem inválido");
                return;
            }

            String messageType = parts[0];

            if (messageType.equals("DISCOVER")) {
                announcePresence();
            } else if (messageType.equals("ANNOUNCE")) {
                int senderId = Integer.parseInt(parts[1]);
                if (!discoveredProcesses.contains(senderId)) {
                    discoveredProcesses.add(senderId);
                    System.out.println("Novo processo descoberto: " + senderId);

                    // Adiciona o endereço do processo descoberto ao map de endereços
                    processAddresses.put(senderId, packet.getAddress().getHostAddress());
                }
            } else {
                String msg = parts[0];
                try {
                    String[] clockParts = parts[1].replaceAll("[\\[\\]\\s]", "").split(",");
                    int[] receivedClock = new int[clockParts.length];
                    for (int i = 0; i < clockParts.length; i++) {
                        receivedClock[i] = Integer.parseInt(clockParts[i]);
                    }
                    if (receivedClock.length != vectorClock.length) {
                        System.out.println("Tamanho do relógio vetorial inválido");
                        return;
                    }
                    for (int i = 0; i < vectorClock.length; i++) {
                        vectorClock[i] = Math.max(vectorClock[i], receivedClock[i]);
                        matrixClock[processId][i] = vectorClock[i];
                    }
                    vectorClock[processId]++;
                    deliver(msg);
                    // Adiciona mensagem recebida ao buffer global
                    buffer.add(received);
                    System.out.println("Relógio lógico atualizado ao receber: " + Arrays.toString(vectorClock));
                    System.out.println("Matriz de relógios atualizada ao receber: " + Arrays.deepToString(matrixClock));
                    // Descartar mensagens estáveis
                    discardStableMessages();
                } catch (NumberFormatException e) {
                    System.out.println("Erro ao analisar o relógio vetorial: " + e.getMessage());
                }
            }
        } catch (java.net.SocketTimeoutException e) {
            // Timeout ocorreu, nenhuma mensagem recebida
        }
    }

    // Método para descartar mensagens estáveis
    private void discardStableMessages() {
        for (String msg : new ArrayList<>(buffer)) {
            String[] parts = msg.split("\\|");
            if (parts.length != 2)
                continue;

            String[] clockParts = parts[1].replaceAll("[\\[\\]\\s]", "").split(",");
            int[] receivedClock = new int[clockParts.length];
            for (int i = 0; i < clockParts.length; i++) {
                receivedClock[i] = Integer.parseInt(clockParts[i]);
            }
            int senderId = Integer.parseInt(clockParts[0]); // Pegando o id do remetente corretamente

            // Verifica se a mensagem está estável
            boolean stable = true;
            for (int i = 0; i < matrixClock.length; i++) {
                if (matrixClock[i].length <= senderId || matrixClock[i][senderId] < receivedClock[senderId]) {
                    stable = false;
                    break;
                }
            }

            if (stable) {
                buffer.remove(msg);
                System.out.println("Mensagem estável descartada: " + msg);
            }
        }
    }

    public void stop() {
        running.set(false);
        socket.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Uso: java StableMulticast <processId> <processAddress> <port> <numProcesses>");
            return;
        }

        int processId = Integer.parseInt(args[0]);
        String processAddress = args[1];
        String groupAddress = "230.0.0.0"; // Endereço de multicast fixo
        int port = Integer.parseInt(args[2]);
        int numProcesses = Integer.parseInt(args[3]);

        // Map de endereços dos processos
        Map<Integer, String> processAddresses = new HashMap<>();
        processAddresses.put(processId, processAddress); // Adiciona o endereço do processo atual
        // Adicione mais endereços conforme necessário

        StableMulticast sm = new StableMulticast(processId, groupAddress, port, numProcesses, processAddresses);

        // Iniciar descoberta de instâncias
        sm.discoverInstances();

        // Aguardar um pouco para a descoberta inicial
        Thread.sleep(2000);

        // Anunciar presença
        sm.announcePresence();

        Thread receiverThread = new Thread(() -> {
            try {
                while (sm.running.get()) {
                    sm.receiveMulticast();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        receiverThread.start();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Processos descobertos: " + sm.discoveredProcesses);
            System.out.println(
                    "Digite 'm' para enviar mensagem multicast, 'u' para enviar mensagem unicast, 'd' para enviar mensagens atrasadas, ou 'exit' para sair:");
            String command = scanner.nextLine();

            if (command.equals("m")) {
                System.out.println("Digite a mensagem multicast:");
                String msg = scanner.nextLine();

                System.out.println("Deseja enviar a mensagem agora (s/n)?");
                String sendNow = scanner.nextLine();

                if (sendNow.equalsIgnoreCase("s")) {
                    sm.msend(msg, sm);
                } else {
                    sm.delayedMessages.add(new DelayedMessage(msg, -1, true, sm.vectorClock.clone()));
                    System.out.println("Mensagem multicast adicionada à lista de mensagens atrasadas.");
                }
            } else if (command.equals("u")) {
                System.out.println("Digite o ID do processo de destino:");
                int targetProcessId = Integer.parseInt(scanner.nextLine());
                System.out.println("Processo alvo: " + targetProcessId);

                System.out.println("Digite a mensagem unicast:");
                String msg = scanner.nextLine();

                System.out.println("Deseja enviar a mensagem agora (s/n)?");
                String sendNow = scanner.nextLine();

                if (sendNow.equalsIgnoreCase("s")) {
                    sm.usend(msg, targetProcessId);
                } else {
                    sm.delayedMessages.add(new DelayedMessage(msg, targetProcessId, false, sm.vectorClock.clone()));
                    System.out.println("Mensagem unicast adicionada à lista de mensagens atrasadas.");
                }
            } else if (command.equals("d")) {
                sm.sendDelayedMessages();
            } else if (command.equals("exit")) {
                sm.stop();
                receiverThread.join();
                break;
            }
        }

        scanner.close();
    }

    public static class DelayedMessage {
        String message;
        int targetProcessId;
        boolean isMulticast;
        int[] vectorClock;

        public DelayedMessage(String message, int targetProcessId, boolean isMulticast, int[] vectorClock) {
            this.message = message;
            this.targetProcessId = targetProcessId;
            this.isMulticast = isMulticast;
            this.vectorClock = vectorClock;
        }
    }
}
