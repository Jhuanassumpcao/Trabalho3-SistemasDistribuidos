import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class StableMulticast {
    private int processId;
    private int[] vectorClock;
    private MulticastSocket multicastSocket;
    private DatagramSocket unicastSocket;
    private InetAddress group;
    private int multicastPort;
    private int unicastPort;
    private AtomicBoolean running;
    private List<Integer> discoveredProcesses;
    private static int currentProcessId = 0;
    private static final Set<Integer> usedIds = new HashSet<>();
    private int[][] matrixClock;
    private List<String> buffer;
    private Map<Integer, String> processAddresses;
    private Map<Integer, Integer> processUnicastPorts;
    private List<DelayedMessage> delayedMessages;
    private static final String GROUP_ADDRESS = "230.0.0.0";
    private static final int MULTICAST_PORT = 5000;
    private static final int NUM_PROCESSES = 3;
    private final IStableMulticast client;

    @SuppressWarnings("deprecation")
    public StableMulticast(String ip, Integer port, IStableMulticast client) throws Exception {
        this.processId = generateUniqueProcessId();
        this.vectorClock = new int[NUM_PROCESSES];
        this.matrixClock = new int[NUM_PROCESSES][NUM_PROCESSES];
        this.multicastSocket = new MulticastSocket(MULTICAST_PORT);
        this.unicastSocket = new DatagramSocket(port);
        this.group = InetAddress.getByName(GROUP_ADDRESS);
        this.multicastSocket.joinGroup(this.group);
        this.multicastPort = MULTICAST_PORT;
        this.unicastPort = port;
        this.running = new AtomicBoolean(true);
        this.discoveredProcesses = new ArrayList<>();
        this.buffer = new ArrayList<>();
        this.processAddresses = new HashMap<>();
        this.processUnicastPorts = new HashMap<>();
        this.delayedMessages = new ArrayList<>();
        this.processAddresses.put(processId, ip);
        this.processUnicastPorts.put(processId, port);
        this.client = client;
        System.out.println("Processo " + processId + " conectado ao grupo multicast " + GROUP_ADDRESS + ":" + MULTICAST_PORT);
        receive();
        discoverInstances();
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

    public void msend() {
        Scanner scanner = new Scanner(System.in);

        try {
            while (true) {
                System.out.println("Escolha uma ação: \n1. Enviar mensagem multicast\n2. Enviar mensagem unicast\n3. Sair");
                String choice = scanner.nextLine();

                if (choice.equals("1")) {
                    System.out.println("Digite a mensagem multicast:");
                    String msg = scanner.nextLine();
                    sendMulticast(msg);
                } else if (choice.equals("2")) {
                    System.out.println("Digite o ID do processo destino:");
                    int targetProcessId = Integer.parseInt(scanner.nextLine());
                    System.out.println("Digite a mensagem unicast:");
                    String msg = scanner.nextLine();
                    sendUnicast(msg, targetProcessId);
                } else if (choice.equals("3")) {
                    break;
                } else {
                    System.out.println("Opção inválida. Tente novamente.");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanner.close();
            try {
                close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void sendMulticast(String msg) throws Exception {
        vectorClock[processId]++;
        matrixClock[processId][processId] = vectorClock[processId];

        String messageWithClock = msg + "|" + Arrays.toString(vectorClock);
        byte[] buffer = messageWithClock.getBytes();
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, multicastPort);
        multicastSocket.send(packet);
        System.out.println("Mensagem enviada: " + msg);
        System.out.println("Buffer enviado: " + Arrays.toString(buffer));
        System.out.println("Relógio lógico ao enviar: " + Arrays.toString(vectorClock));
        System.out.println("Matriz de relógios ao enviar: " + Arrays.deepToString(matrixClock));
    }

    private void sendUnicast(String msg, int targetProcessId) throws Exception {
        vectorClock[processId]++;
        matrixClock[processId][processId] = vectorClock[processId];

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

    public void sendDelayedMessages() throws Exception {
        delayedMessages.sort((m1, m2) -> {
            for (int i = 0; i < m1.vectorClock.length; i++) {
                if (m1.vectorClock[i] != m2.vectorClock[i]) {
                    return Integer.compare(m1.vectorClock[i], m2.vectorClock[i]);
                }
            }
            return 0;
        });

        for (DelayedMessage delayedMessage : new ArrayList<>(delayedMessages)) {
            if (delayedMessage.isMulticast) {
                sendMulticast(delayedMessage.message);
            } else {
                sendUnicast(delayedMessage.message, delayedMessage.targetProcessId);
            }
            delayedMessages.remove(delayedMessage);
        }
        discardStableMessages();
    }

    public void discoverInstances() throws Exception {
        String discoveryMessage = "DISCOVER|" + processId;
        byte[] buffer = discoveryMessage.getBytes();
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, multicastPort);
        multicastSocket.send(packet);
        System.out.println("Enviada mensagem de descoberta");
        System.out.println("Buffer enviado: " + Arrays.toString(buffer));
    }

    public void announcePresence() throws Exception {
        String announceMessage = "ANNOUNCE|" + processId + "|" + unicastPort;
        byte[] buffer = announceMessage.getBytes();
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, multicastPort);
        multicastSocket.send(packet);
        System.out.println("Anunciada presença do processo " + processId);
        System.out.println("Buffer enviado: " + Arrays.toString(buffer));
    }

    public void receiveMulticast() throws Exception {
        byte[] packetBuffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(packetBuffer, packetBuffer.length);
        multicastSocket.setSoTimeout(1000);
        try {
            multicastSocket.receive(packet);
            int length = packet.getLength();
            byte[] data = Arrays.copyOf(packet.getData(), length);
            String received = new String(data, 0, length);
            System.out.println("Pacote multicast recebido: " + received);
            System.out.println("Buffer recebido: " + Arrays.toString(data));
            String[] parts = received.split("\\|");
            if (parts.length < 2) return;
    
            String messageType = parts[0];
            if (messageType.equals("DISCOVER")) {
                announcePresence();
            } else if (messageType.equals("ANNOUNCE")) {
                int senderId = Integer.parseInt(parts[1]);
                int senderUnicastPort = Integer.parseInt(parts[2]);
                if (!discoveredProcesses.contains(senderId)) {
                    discoveredProcesses.add(senderId);
                    processAddresses.put(senderId, packet.getAddress().getHostAddress());
                    processUnicastPorts.put(senderId, senderUnicastPort);
                    System.out.println("Novo processo descoberto: " + senderId);
                }
                System.out.println("Anunciada presença do processo " + senderId);
            } else {
                handleReceivedMessage(received);
            }
        } catch (java.net.SocketTimeoutException e) {
        }
        discardStableMessages();
    }
    
    public void receiveUnicast() throws Exception {
        byte[] packetBuffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(packetBuffer, packetBuffer.length);
        unicastSocket.setSoTimeout(1000);
        try {
            unicastSocket.receive(packet);
            int length = packet.getLength();
            byte[] data = Arrays.copyOf(packet.getData(), length);
            String received = new String(data, 0, length);
            System.out.println("Pacote unicast recebido: " + received);
            System.out.println("Buffer recebido: " + Arrays.toString(data));
            handleReceivedMessage(received);
        } catch (java.net.SocketTimeoutException e) {
        }
        discardStableMessages();
    }
    

    private void handleReceivedMessage(String received) {
        String[] parts = received.split("\\|");
        if (parts.length < 2) return;

        String message = parts[0];
        int[] receivedVectorClock = Arrays.stream(parts[1].replaceAll("[\\[\\] ]", "").split(","))
                .mapToInt(Integer::parseInt).toArray();

        System.out.println("Mensagem recebida: " + message);
        System.out.println("Relógio lógico recebido: " + Arrays.toString(receivedVectorClock));

        if (isDeliverable(receivedVectorClock)) {
            this.client.deliver(message);
            vectorClock[processId]++;
            updateMatrixClock(receivedVectorClock);
        } else {
            delayedMessages.add(new DelayedMessage(message, receivedVectorClock, false, -1));
        }
    }

    private boolean isDeliverable(int[] receivedVectorClock) {
        for (int i = 0; i < NUM_PROCESSES; i++) {
            if (receivedVectorClock[i] > matrixClock[processId][i]) {
                return false;
            }
        }
        return true;
    }

    private void updateMatrixClock(int[] receivedVectorClock) {
        for (int i = 0; i < NUM_PROCESSES; i++) {
            matrixClock[processId][i] = Math.max(matrixClock[processId][i], receivedVectorClock[i]);
        }
        System.out.println("Matriz de relógios atualizada: " + Arrays.deepToString(matrixClock));
    }

    public void receive() {
        new Thread(() -> {
            while (running.get()) {
                try {
                    receiveMulticast();
                    receiveUnicast();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public void close() throws Exception {
        running.set(false);
        multicastSocket.leaveGroup(group);
        multicastSocket.close();
        unicastSocket.close();
    }

    private void discardStableMessages() {
        for (Iterator<DelayedMessage> iterator = delayedMessages.iterator(); iterator.hasNext(); ) {
            DelayedMessage delayedMessage = iterator.next();
            if (isDeliverable(delayedMessage.vectorClock)) {
                this.client.deliver(delayedMessage.message);
                vectorClock[processId]++;
                updateMatrixClock(delayedMessage.vectorClock);
                iterator.remove();
                System.out.println("Mensagem removida: " + delayedMessage.message);
            }
        }
    }
    

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
