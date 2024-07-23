import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.DatagramSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
     private static final Set<Integer> usedIds = new HashSet<>();
    private int[][] matrixClock; // Matriz de Relógios Vetoriais (MC)
    private List<String> buffer; // Lista de mensagens recebidas
    private Map<Integer, String> processAddresses; // Map de endereços dos processos
    private Map<Integer, Integer> processUnicastPorts; // Map de portas unicast dos processos
    private List<DelayedMessage> delayedMessages; // Lista de mensagens atrasadas
    private static final String GROUP_ADDRESS = "230.0.0.0"; // Mocked group address
    private static final int MULTICAST_PORT = 5000; // Mocked multicast port
    private static final int NUM_PROCESSES = 3; // Tamanho do vetor fixado

    @SuppressWarnings("deprecation")
    public StableMulticast(String ip, Integer port, IStableMulticast client) throws Exception {
        this.processId = generateUniqueProcessId(); // Gera um ID único
        this.vectorClock = new int[NUM_PROCESSES];
        this.matrixClock = new int[NUM_PROCESSES][NUM_PROCESSES]; // inicializa a matriz de relógios vetoriais
        this.multicastSocket = new MulticastSocket(MULTICAST_PORT);
        this.unicastSocket = new DatagramSocket(port);
        this.group = InetAddress.getByName(GROUP_ADDRESS);
        this.multicastSocket.joinGroup(this.group);
        this.multicastPort = MULTICAST_PORT;
        this.unicastPort = port;
        this.running = new AtomicBoolean(true);
        this.discoveredProcesses = new ArrayList<>();
        this.discoveredProcesses.add(processId);
        this.buffer = new ArrayList<>(); // Inicializa o buffer de mensagens
        this.processAddresses = new HashMap<>();
        this.processUnicastPorts = new HashMap<>();
        this.delayedMessages = new ArrayList<>(); // Inicializa a lista de mensagens atrasadas
        this.processAddresses.put(processId, ip); // Inicializa o map de endereços
        this.processUnicastPorts.put(processId, port); // Inicializa o map de portas unicast
        System.out.println("Processo " + processId + " conectado ao grupo multicast " + GROUP_ADDRESS + ":" + MULTICAST_PORT);
    }
    private synchronized int generateUniqueProcessId() {
        Random rand = new Random();
        int id;
        do {
            id = rand.nextInt(NUM_PROCESSES);
        } while (usedIds.contains(id));
        usedIds.add(id);
        return id;
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
            System.out.println("Buffer recebido: " + Arrays.toString(Arrays.copyOf(packet.getData(), packet.getLength())));

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
                System.out.println("Anunciada presença do processo " + senderId);
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
                    // Adiciona a mensagem recebida ao buffer
                    buffer.add(received);
                } catch (NumberFormatException e) {
                    System.out.println("Formato de relógio vetorial inválido");
                }
            }
        } catch (Exception e) {
            // Timeout ou erro ao receber pacote
        }
    }

    private void discardStableMessages() {
        for (String msg : new ArrayList<>(buffer)) {
            String[] parts = msg.split("\\|");
            if (parts.length != 2) continue;
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

    // Método para receber mensagens unicast
    public void receiveUnicast() throws Exception {
        byte[] packetBuffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(packetBuffer, packetBuffer.length);
        unicastSocket.setSoTimeout(1000);
        try {
            unicastSocket.receive(packet);
            String received = new String(packet.getData(), 0, packet.getLength());
            System.out.println("Pacote unicast recebido: " + received);
            System.out.println("Buffer recebido: " + Arrays.toString(Arrays.copyOf(packet.getData(), packet.getLength())));

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
                // Adiciona a mensagem recebida ao buffer
                buffer.add(received);
            } catch (NumberFormatException e) {
                System.out.println("Formato de relógio vetorial inválido");
            }
        } catch (Exception e) {
            // Timeout ou erro ao receber pacote
        }
    }

    public void startReceiving() {
        Thread multicastThread = new Thread(() -> {
            while (running.get()) {
                try {
                    receiveMulticast();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        Thread unicastThread = new Thread(() -> {
            while (running.get()) {
                try {
                    receiveUnicast();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        multicastThread.start();
        unicastThread.start();
    }

    public void stopReceiving() {
        running.set(false);
        multicastSocket.close();
        unicastSocket.close();
    }

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter IP address: ");
        String ip = scanner.nextLine();
        System.out.print("Enter port: ");
        int port = scanner.nextInt();
        scanner.nextLine();  // Consome a nova linha após a entrada do número
        IStableMulticast client = new IStableMulticast() {
            @Override
            public void deliver(String msg) {
                System.out.println("Delivered: " + msg);
            }
        };
        StableMulticast stableMulticast = new StableMulticast(ip, port, client);
        stableMulticast.startReceiving();
        
        stableMulticast.discoverInstances();
        stableMulticast.announcePresence();

        while (true) {
            System.out.print("Enter message: ");
            String msg = scanner.nextLine();
            if (msg.equalsIgnoreCase("exit")) {
                stableMulticast.stopReceiving();
                break;
            } else if (msg.equalsIgnoreCase("d")) {
                stableMulticast.sendDelayedMessages();
            } else if (msg.equalsIgnoreCase("m")) {
                System.out.print("Enter multicast message: ");
                msg = scanner.nextLine();
                stableMulticast.msend(msg);
            } else if (msg.equalsIgnoreCase("u")) {
                System.out.print("Enter unicast message: ");
                msg = scanner.nextLine();
                System.out.print("Enter target process ID: ");
                int targetProcessId = scanner.nextInt();
                scanner.nextLine();  // Consome a nova linha após a entrada do número
                stableMulticast.usend(msg, targetProcessId);
            } else {
                System.out.println("Comando desconhecido: " + msg);
            }
        }
        scanner.close();
    }

    // Classe interna para armazenar mensagens atrasadas
    private class DelayedMessage {
        String message;
        int[] vectorClock;
        boolean isMulticast;
        int targetProcessId;

        DelayedMessage(String message, int[] vectorClock, boolean isMulticast, int targetProcessId) {
            this.message = message;
            this.vectorClock = vectorClock;
            this.isMulticast = isMulticast;
            this.targetProcessId = targetProcessId;
        }
    }
}
