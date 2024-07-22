import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.DatagramSocket;
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
    private MulticastSocket multicastSocket;
    private DatagramSocket unicastSocket;
    private InetAddress group;
    private int multicastPort;
    private int unicastPort;
    private AtomicBoolean running;
    private List<Integer> discoveredProcesses;
    private int[][] matrixClock; // Matriz de Relógios Vetoriais (MC)
    private List<String> buffer; // Lista de mensagens recebidas
    private Map<Integer, String> processAddresses; // Map de endereços dos processos
    private Map<Integer, Integer> processUnicastPorts; // Map de portas unicast dos processos
    private List<DelayedMessage> delayedMessages; // Lista de mensagens atrasadas

    @SuppressWarnings("deprecation")
    public StableMulticast(int processId, String groupAddress, int multicastPort, int unicastPort, int numProcesses,
            Map<Integer, String> processAddresses, Map<Integer, Integer> processUnicastPorts) throws Exception {
        this.processId = processId;
        this.vectorClock = new int[numProcesses];
        this.matrixClock = new int[numProcesses][numProcesses]; // inicializa a matriz de relógios vetoriais
        this.multicastSocket = new MulticastSocket(multicastPort);
        this.unicastSocket = new DatagramSocket(unicastPort);
        this.group = InetAddress.getByName(groupAddress);
        this.multicastSocket.joinGroup(this.group);
        this.multicastPort = multicastPort;
        this.unicastPort = unicastPort;
        this.running = new AtomicBoolean(true);
        this.discoveredProcesses = new ArrayList<>();
        this.discoveredProcesses.add(processId);
        this.buffer = new ArrayList<>(); // Inicializa o buffer de mensagens
        this.processAddresses = processAddresses; // Inicializa o map de endereços
        this.processUnicastPorts = processUnicastPorts; // Inicializa o map de portas unicast
        this.delayedMessages = new ArrayList<>(); // Inicializa a lista de mensagens atrasadas
        System.out.println("Processo " + processId + " conectado ao grupo multicast " + groupAddress + ":" + multicastPort);
    }

    @Override
    public void deliver(String msg) {
        System.out.println("Mensagem recebida: " + msg);
    }

    // Método para enviar mensagem multicast
    public void msend(String msg) throws Exception {
        // atualiza o relógio vetorial e a matriz de relógios
        vectorClock[processId]++;
        matrixClock[processId][processId] = vectorClock[processId];

        // envia a mensagem multicast
        String messageWithClock = msg + "|" + Arrays.toString(vectorClock);
        byte[] buffer = messageWithClock.getBytes();
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, multicastPort);
        multicastSocket.send(packet);
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

        InetAddress targetAddress = InetAddress.getByName(processAddresses.get(targetProcessId));
        int targetPort = processUnicastPorts.get(targetProcessId);
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, targetAddress, targetPort);
        unicastSocket.send(packet);
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
                msend(delayedMessage.message);
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
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, multicastPort);
        multicastSocket.send(packet);
        System.out.println("Enviada mensagem de descoberta");
    }

    // Método para anunciar que o processo está presente
    public void announcePresence() throws Exception {
        String announceMessage = "ANNOUNCE|" + processId + "|" + unicastPort;
        byte[] buffer = announceMessage.getBytes();
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, multicastPort);
        multicastSocket.send(packet);
        System.out.println("Anunciada presença do processo " + processId);
    }

    // Método para receber mensagens multicast
    public void receiveMulticast() throws Exception {
        byte[] packetBuffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(packetBuffer, packetBuffer.length);
        multicastSocket.setSoTimeout(1000);
        try {
            multicastSocket.receive(packet);
            String received = new String(packet.getData(), 0, packet.getLength());
            System.out.println("Pacote multicast recebido: " + received);
            System.out.println(
                    "Buffer recebido: " + Arrays.toString(Arrays.copyOf(packet.getData(), packet.getLength())));

            String[] parts = received.split("\\|");
            if (parts.length < 2) {
                System.out.println("Formato de mensagem inválido");
                return;
            }

            String messageType = parts[0];

            if (messageType.equals("DISCOVER")) {
                announcePresence();
            } else if (messageType.equals("ANNOUNCE")) {
                int senderId = Integer.parseInt(parts[1]);
                int senderUnicastPort = Integer.parseInt(parts[2]);
                if (!discoveredProcesses.contains(senderId)) {
                    discoveredProcesses.add(senderId);
                    System.out.println("Novo processo descoberto: " + senderId);

                    // Adiciona o endereço e a porta unicast do processo descoberto aos maps
                    processAddresses.put(senderId, packet.getAddress().getHostAddress());
                    processUnicastPorts.put(senderId, senderUnicastPort);
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

    // Método para receber mensagens unicast
    public void receiveUnicast() throws Exception {
        byte[] packetBuffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(packetBuffer, packetBuffer.length);
        unicastSocket.setSoTimeout(1000);
        try {
            unicastSocket.receive(packet);
            String received = new String(packet.getData(), 0, packet.getLength());
            System.out.println("Pacote unicast recebido: " + received);
            System.out.println(
                    "Buffer recebido: " + Arrays.toString(Arrays.copyOf(packet.getData(), packet.getLength())));

            String[] parts = received.split("\\|");
            if (parts.length < 2) {
                System.out.println("Formato de mensagem inválido");
                return;
            }

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
        } catch (java.net.SocketTimeoutException e) {
            // Timeout ocorreu, nenhuma mensagem recebida
        }
    }

    // Método para descartar mensagens estáveis
    private void discardStableMessages() {
        // Implemente a lógica para descartar mensagens estáveis aqui
    }

    public void stop() {
        running.set(false);
        multicastSocket.close();
        unicastSocket.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.out.println("Uso: java StableMulticast <processId> <processAddress> <multicastPort> <unicastPort> <numProcesses>");
            return;
        }

        int processId = Integer.parseInt(args[0]);
        String processAddress = args[1];
        String groupAddress = "230.0.0.0";
        int multicastPort = Integer.parseInt(args[2]);
        int unicastPort = Integer.parseInt(args[3]);
        int numProcesses = Integer.parseInt(args[4]);

        Map<Integer, String> processAddresses = new HashMap<>();
        processAddresses.put(processId, processAddress);

        Map<Integer, Integer> processUnicastPorts = new HashMap<>();
        processUnicastPorts.put(processId, unicastPort);

        StableMulticast sm = new StableMulticast(processId, groupAddress, multicastPort, unicastPort, numProcesses,
                processAddresses, processUnicastPorts);

        sm.discoverInstances();
        Thread.sleep(2000);
        sm.announcePresence();

        Thread receiverThread = new Thread(() -> {
            try {
                while (sm.running.get()) {
                    sm.receiveMulticast();
                    sm.receiveUnicast();
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
                    sm.msend(msg);
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
}

// Classe para representar uma mensagem atrasada
class DelayedMessage {
    String message;
    int targetProcessId;
    boolean isMulticast;
    int[] vectorClock;

    DelayedMessage(String message, int targetProcessId, boolean isMulticast, int[] vectorClock) {
        this.message = message;
        this.targetProcessId = targetProcessId;
        this.isMulticast = isMulticast;
        this.vectorClock = vectorClock;
    }
}
