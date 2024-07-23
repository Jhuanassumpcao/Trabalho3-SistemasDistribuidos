import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.DatagramSocket;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.ArrayList;
import java.util.List;

public class StableMulticast {
    private AtomicBoolean running;

    private int[][] matrixClock; // Matriz de Relógios Vetoriais (MC)
    private int[] vectorClock;

    private List<String> buffer; // Lista de mensagens recebidas
    private List<DelayedMessage> delayedMessages; // Lista de mensagens atrasadas

    private List<String> processesAdresses; // Set de processos descobertos

    private MulticastSocket multicastSocket;
    private InetAddress group;
    private int multicastPort = 5000;

    private DatagramSocket unicastSocket;
    private int unicastPort;

    private int numProcesses = 3;
    private String groupAddress = "230.0.0.0";

    private int processId;
    IStableMulticast client;

    @SuppressWarnings("deprecation")
    public StableMulticast(String ip, Integer port, IStableMulticast client) throws Exception {
        this.running = new AtomicBoolean(true);
        this.vectorClock = new int[numProcesses];
        this.matrixClock = new int[numProcesses][numProcesses]; // inicializa a matriz de relógios vetoriais

        this.multicastSocket = new MulticastSocket(multicastPort);
        this.group = InetAddress.getByName(groupAddress);
        this.multicastSocket.joinGroup(this.group);

        this.unicastPort = port;
        this.unicastSocket = new DatagramSocket(port);

        this.buffer = new ArrayList<>(); // Inicializa o buffer de mensagens

        this.processesAdresses = new ArrayList<>(); // Inicializa o set de endereços de processos descobertos
        this.addProcess(ip, port);

        this.delayedMessages = new ArrayList<>(); // Inicializa a lista de mensagens atrasadas
        this.processId = 0;
        this.client = client;

        System.out.println(
                "Processo " + ip + ":" + port + " entrou no grupo " + groupAddress + ":" + multicastPort);

        discoverInstances();
        announcePresence();
    }

    private void addProcess(String ip, Integer port) {
        this.processesAdresses.add(ip + ":" + port);

    }

    private boolean isProcessDiscovered(String ip, Integer port) {
        return this.processesAdresses.contains(ip + ":" + port);
    }

    private int getProcessId(String ip, Integer port) {
        return this.processesAdresses.indexOf(ip + ":" + port);
    }

    public void insertDelayedMessage(String message, String targetIp, int targetPort, boolean isMulticast,
            int[] vectorClock) {
        this.delayedMessages.add(new DelayedMessage(message, targetIp, targetPort, isMulticast, vectorClock));
    }

    public void printActiveProcesses() {
        System.out.println("Processos ativos:");
        for (int i = 0; i < processesAdresses.size(); i++) {
            System.out.println(i + " - " + processesAdresses.get(i));
        }
    }

    // Método para enviar mensagem multicast
    public void msend(String msg) throws Exception {
        // atualiza o relógio vetorial e a matriz de relógios
        vectorClock[processId]++;
        matrixClock[processId][processId] = vectorClock[processId];

        // envia a mensagem multicast
        String messageWithClock = msg + "|" + Arrays.toString(vectorClock) + "|" + processesAdresses.toString() + "|"
                + unicastPort;
        byte[] buffer = messageWithClock.getBytes();

        for (String processAddress : processesAdresses) {
            String[] parts = processAddress.split(":");
            InetAddress targetAddress = InetAddress.getByName(parts[0]);
            int targetPort = Integer.parseInt(parts[1]);

            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, targetAddress, targetPort);
            unicastSocket.send(packet);
        }

        System.out.println("Mensagem multicast enviada: " + msg);
        System.out.println("Buffer enviado: " + Arrays.toString(buffer));
        System.out.println("Relógio lógico ao enviar: " + Arrays.toString(vectorClock));
        System.out.println("Matriz de relógios ao enviar: " + Arrays.deepToString(matrixClock));
    }

    // Método para enviar mensagem unicast
    public void usend(String msg, String targetIp, int targetPort) throws Exception {
        // atualiza o relógio vetorial e a matriz de relógios
        vectorClock[processId]++;
        matrixClock[processId][processId] = vectorClock[processId];

        // envia a mensagem unicast
        String messageWithClock = msg + "|" + Arrays.toString(vectorClock) + "|" + processesAdresses.toString() + "|"
                + unicastPort;
        byte[] buffer = messageWithClock.getBytes();

        InetAddress targetAddress = InetAddress.getByName(targetIp);
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, targetAddress, targetPort);
        unicastSocket.send(packet);

        System.out.println("Mensagem unicast enviada: " + msg);
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
                usend(delayedMessage.message, delayedMessage.targetIp, delayedMessage.targetPort);
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
        String announceMessage = "ANNOUNCE|" + unicastPort;
        byte[] buffer = announceMessage.getBytes();
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, multicastPort);
        multicastSocket.send(packet);
        System.out.println("Anunciada presença do processo " + processId);
    }

    // Monta os itens de relógio recebido, de acordo com as posições de cada
    // processo
    public int[] mountReceivedClock(String[] clockItems, String[] processes) {
        int[] receivedClock = new int[numProcesses];

        for (int i = 0; i < processes.length; i++) {
            int localIndex = processesAdresses.indexOf(processes[i]);
            receivedClock[localIndex] = Integer.parseInt(clockItems[i]);
        }

        return receivedClock;
    }

    // Método para receber mensagens multicast
    public void receiveMulticast() throws Exception {
        byte[] packetBuffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(packetBuffer, packetBuffer.length);
        multicastSocket.setSoTimeout(1000);
        try {
            multicastSocket.receive(packet);
            String received = new String(packet.getData(), 0, packet.getLength());
            System.out.println("Pacote multicast recebido:\n " + received);
            System.out.println(
                    "Buffer recebido:\n " + Arrays.toString(Arrays.copyOf(packet.getData(), packet.getLength())));

            String[] parts = received.split("\\|");
            if (parts.length < 2) {
                System.out.println("Formato de mensagem inválido");
                return;
            }

            String messageType = parts[0];

            if (messageType.equals("DISCOVER")) {
                announcePresence();
            } else if (messageType.equals("ANNOUNCE")) {
                String senderIp = packet.getAddress().getHostAddress();
                int senderUnicastPort = Integer.parseInt(parts[1]);

                if (!isProcessDiscovered(senderIp, senderUnicastPort)) {
                    // Adiciona o endereço e a porta unicast do processo descoberto ao set
                    this.addProcess(packet.getAddress().getHostAddress(), senderUnicastPort);

                    System.out.println("Novo processo descoberto: " + getProcessId(senderIp, senderUnicastPort));
                    printActiveProcesses();
                }
            } else {
                String msg = parts[0];
                String senderIp = packet.getAddress().getHostAddress();
                int senderUnicastPort = Integer.parseInt(parts[3]);

                try {
                    String[] clockParts = parts[1].replaceAll("[\\[\\]\\s]", "").split(",");
                    String[] processes = parts[2].replaceAll("[\\[\\]\\s]", "").split(",");

                    int[] receivedClock = mountReceivedClock(clockParts, processes);
                    if (receivedClock.length != numProcesses) {
                        System.out.println("Tamanho do relógio vetorial inválido");
                        return;
                    }

                    matrixClock[getProcessId(senderIp, senderUnicastPort)] = receivedClock;
                    vectorClock[processId]++;
                    for (int i = 0; i < vectorClock.length; i++) {
                        vectorClock[i] = Math.max(vectorClock[i], receivedClock[i]);
                        matrixClock[processId][i] = vectorClock[i];
                    }

                    // Adiciona mensagem recebida ao buffer global
                    buffer.add(received);
                    client.deliver(msg);
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
            System.out.println("Pacote unicast recebido:\n " + received);
            System.out.println(
                    "Buffer recebido:\n " + Arrays.toString(Arrays.copyOf(packet.getData(), packet.getLength())));

            String[] parts = received.split("\\|");
            if (parts.length < 2) {
                System.out.println("Formato de mensagem inválido");
                return;
            }

            String msg = parts[0];
            String senderIp = packet.getAddress().getHostAddress();
            int senderUnicastPort = Integer.parseInt(parts[3]);

            try {
                String[] clockParts = parts[1].replaceAll("[\\[\\]\\s]", "").split(",");
                String[] processes = parts[2].replaceAll("[\\[\\]\\s]", "").split(",");
                int[] receivedClock = mountReceivedClock(clockParts, processes);

                if (receivedClock.length != numProcesses) {
                    System.out.println(receivedClock.length + " " + numProcesses);
                    System.out.println("Tamanho do relógio vetorial inválido");
                    return;
                }

                matrixClock[getProcessId(senderIp, senderUnicastPort)] = receivedClock;
                vectorClock[processId]++;
                for (int i = 0; i < vectorClock.length; i++) {
                    vectorClock[i] = Math.max(vectorClock[i], receivedClock[i]);
                    matrixClock[processId][i] = vectorClock[i];
                }

                // Adiciona mensagem recebida ao buffer global
                buffer.add(received + "|" + getProcessId(senderIp, senderUnicastPort));
                client.deliver(msg);
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

    private synchronized void discardStableMessages() {
        List<String> stableMessages = new ArrayList<>();
    
        for (String msg : new ArrayList<>(buffer)) {
            String[] parts = msg.split("\\|");
            String[] clockParts = parts[1].replaceAll("[\\[\\]\\s]", "").split(",");
            String[] processes = parts[2].replaceAll("[\\[\\]\\s]", "").split(",");
            int[] receivedClock = mountReceivedClock(clockParts, processes);
            int senderId = Integer.parseInt(parts[4]);
    
            for (int i = 0; i < clockParts.length; i++) {
                receivedClock[i] = Integer.parseInt(clockParts[i]);
            }

            int msgClockValue = receivedClock[senderId];

            int minClockValue = Integer.MAX_VALUE;
            for (int i = 0; i < matrixClock.length; i++) {
                minClockValue = Math.min(minClockValue, matrixClock[i][senderId]);
            }
    
            if (msgClockValue <= minClockValue) {
                stableMessages.add(msg);
            }
        }
    
        for (String stableMsg : stableMessages) {
            buffer.remove(stableMsg);
            System.out.println("Mensagem estável descartada: " + stableMsg);
        }
    }
    

    public void start() throws Exception {
        Thread.sleep(2000);
        Thread receiverThread = new Thread(() -> {
            try {
                while (isRunning()) {
                    receiveMulticast();
                    receiveUnicast();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        receiverThread.start();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            printActiveProcesses();
            System.out.println(
                    "Digite 'm' para enviar mensagem multicast, 'u' para enviar mensagem unicast, 'd' para enviar mensagens atrasadas, ou 'exit' para sair:");
            String command = scanner.nextLine();

            if (command.equals("m")) {
                System.out.println("Digite a mensagem multicast:");
                String msg = scanner.nextLine();

                System.out.println("Deseja enviar a mensagem agora (s/n)?");
                String sendNow = scanner.nextLine();

                if (sendNow.equalsIgnoreCase("s")) {
                    msend(msg);
                } else {
                    insertDelayedMessage(msg, "", -1, true, vectorClock.clone());
                    System.out.println("Mensagem multicast adicionada à lista de mensagens atrasadas.");
                }
            } else if (command.equals("u")) {
                System.out.println("Digite o ID do processo de destino:");
                String targetProcess = scanner.nextLine();
                System.out.println("Processo alvo: " + targetProcess);

                targetProcess = processesAdresses.get(Integer.parseInt(targetProcess));
                String ip = targetProcess.split(":")[0];
                Integer port = Integer.parseInt(targetProcess.split(":")[1]);

                System.out.println("Digite a mensagem unicast:");
                String msg = scanner.nextLine();

                System.out.println("Deseja enviar a mensagem agora (s/n)?");
                String sendNow = scanner.nextLine();

                if (sendNow.equalsIgnoreCase("s")) {
                    usend(msg, ip, port);
                } else {
                    insertDelayedMessage(msg, ip, port, false, vectorClock.clone());
                    System.out.println("Mensagem unicast adicionada à lista de mensagens atrasadas.");
                }
            } else if (command.equals("d")) {
                sendDelayedMessages();
            } else if (command.equals("exit")) {
                stop();
                receiverThread.join();
                break;
            }
        }

        scanner.close();
    }

    public void stop() {
        running.set(false);
        multicastSocket.close();
        unicastSocket.close();
    }

    public Boolean isRunning() {
        return running.get();
    }
}

// Classe para representar uma mensagem atrasada
class DelayedMessage {
    String message;
    String targetIp;
    int targetPort;

    boolean isMulticast;
    int[] vectorClock;

    public DelayedMessage(String message, String targetIp, int targetPort, boolean isMulticast, int[] vectorClock) {
        this.message = message;
        this.targetIp = targetIp;
        this.targetPort = targetPort;
        this.isMulticast = isMulticast;
        this.vectorClock = vectorClock;
    }
}
