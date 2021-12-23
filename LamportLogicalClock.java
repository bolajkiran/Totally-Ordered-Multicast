import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class LamportLogicalClock {

    public static String process_id; //Ex P1
    public static int pid; // Ex. 1

    private static InetAddress server_host_addr = InetAddress.getLoopbackAddress();
    private static ServerSocket server_socket;
    public static int[] process_portList;

    public static String[] process_eventList;
    private static int num_processes;
    // Event buffer for received messages
    public static volatile CopyOnWriteArrayList<ProcessEvent> event_buffer = new CopyOnWriteArrayList<>();
    // Acknowledgement buffer of the events
    public static volatile HashMap<String, HashSet<String>> ack_buffer = new HashMap<>();

    // Logical Clock of the process
    private static int logicalClock_process = 0;

    // total number of events received
    private static int events_delivered = 0;

    // event ID of an send event
    public  static String current_eventID = null;


    public static Thread connectionThread = null;
    public static Thread orderThread = null;
    public static Thread ackThread = null;

    public LamportLogicalClock(int num_processes, int num_events, String processId) {
        pid = Integer.parseInt(processId);
        process_id = "P" + processId;
        process_portList = new int[num_processes];
        for (int i = 0; i < num_processes; i++) {
            process_portList[i] = 2001 + i;
        }
        LamportLogicalClock.num_processes = num_processes;
        process_eventList = new String[num_events];
        for (int i = 0; i < num_events; i++) {
            process_eventList[i] = "m" + i;
        }
        
    }

    // enforce total ordering
    public static Comparator<ProcessEvent> comp = (event1, event2)->{
        if(event1.getLogical_time() != event2.getLogical_time()){
            if(event1.getLogical_time() > event2.getLogical_time()){
                return 1;
            }
        }else{
            if(event1.getPid() > event2.getPid()){
                return 1;
            }
        }

        return -1;
    };

    public void init() throws InterruptedException {
        System.out.println("PROCESS: " + process_id);
        System.out.println("\t\t    ___");
        System.out.println("\t\t   / []\\");
        System.out.println("\t\t _|_____|_");
        System.out.println("\t\t| | === | |");
        System.out.println("\t\t|_|  0  |_|");
        System.out.println("\t\t ||_____||");
        System.out.println("\t\t|~ \\___/ ~|");
        System.out.println("\t\t/=\\ /=\\ /=\\");
        System.out.println("\t\t[_] [_] [_]");
        System.out.println("\nEvents delivered to " + process_id + " are\uD83D\uDE0A :");
        System.out.println("------------------------------");

        // Thread 01: manageConnections
        connectionThread = (new Thread(() -> {
            manageConnections();
            return;
        }));

        connectionThread.start();

        // Thread 02: enforceTotalOrder
        orderThread = (new Thread(() -> {
            enforceTotalOrder();
            return;
        }));

        orderThread.start();

        // Thread 03: manageAcknowledgements
        ackThread = (new Thread(() -> {
            manageAcknowledgements();
            return;
        }));

        ackThread.start();

        try {
            // Added this delay to wait till all other processes are in the listening state
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        for (int i = pid; i < process_eventList.length;) {
            current_eventID = process_eventList[i];
            ProcessEvent myEvent = new ProcessEvent();
            myEvent.setAcknowledged(false);
            myEvent.setEvent_id(current_eventID);
            myEvent.setLogical_time(logicalClock_process);
            myEvent.setPid(pid);
            myEvent.setProcess_id(process_id);

            // Multicast event to everyone
            new Thread(() -> {
            sendEvent(myEvent);
            }).start();
            //Thread.sleep(800);
            i = i + num_processes;
        }

    }

    public void manageConnections(){
        int server_port = process_portList[pid];
        try {
            server_socket = new ServerSocket(server_port, 0, server_host_addr);
            //while(events_delivered != process_eventList.length){
            while(true){
                Socket clientSocket = server_socket.accept();
                ObjectInputStream obj_ip = new ObjectInputStream(clientSocket.getInputStream());
                // receive an event
                ProcessEvent event = (ProcessEvent)obj_ip.readObject();
                clientSocket.close();
                (new Thread(() -> {
                    manageBuffers(event);
                    return;
                })).start();
            }
            /*if(!server_socket.isClosed()){
                server_socket.close();
            }*/
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized void manageBuffers(ProcessEvent event){
        try {
            // is this an event acknowledgement the process is receiving?
            if(event.isAcknowledged()){
                if(ack_buffer.containsKey(event.getEvent_id())){
                    ack_buffer.get(event.getEvent_id()).add(event.getProcess_id());
                }else{
                    HashSet<String> sender_set = new HashSet<>();
                    sender_set.add(event.getProcess_id());
                    ack_buffer.put(event.getEvent_id(), sender_set);
                }
            } else{
                // is it send message event which the process is receiving
                logicalClock_process = Math.max(logicalClock_process, event.getLogical_time()) + 1;
                event_buffer.add(event);
                Collections.sort(event_buffer, comp);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void manageAcknowledgements(){
        // manage acknowledgments till all the events are delivered to the process
        //while(events_delivered != process_eventList.length){
        while(true){
            synchronized (event_buffer) {
                if (!event_buffer.isEmpty()) {
                    // get first event in received buffer
                    ProcessEvent event = event_buffer.get(0);
                    if (checkWhetherWeCanSendAckOfThisEvent(event)) {
                        ProcessEvent myEvent = new ProcessEvent();
                        myEvent.setAcknowledged(true);
                        myEvent.setEvent_id(event.getEvent_id());
                        myEvent.setLogical_time(logicalClock_process);
                        myEvent.setPid(pid);
                        myEvent.setProcess_id(process_id);
                        sendEvent(myEvent);

                        if (ack_buffer.containsKey(event.getEvent_id())) {
                            ack_buffer.get(event.getEvent_id()).add(process_id);
                        } else {
                            HashSet<String> sender_set = new HashSet<>();
                            sender_set.add(process_id);
                            ack_buffer.put(event.getEvent_id(), sender_set);
                        }
                    }
                }
            }
        }
    }

    public void enforceTotalOrder(){
        // enforce total ordering till all the events are delivered to the process
        //while(events_delivered != process_eventList.length){
        while(true){
            synchronized (event_buffer) {
                if (!event_buffer.isEmpty()) {
                    // get the first event in received buffer
                    ProcessEvent event = event_buffer.get(0);
                    if (ack_buffer.containsKey(event.getEvent_id())) {
                        if (ack_buffer.get(event.getEvent_id()).size() == num_processes) {
                            // if the acknowledgement of an event is sent to all the processes, event can be delivered
                            deliverEvent(event);

                            // once event is delivered remove from the received queue/buffer
                            event_buffer.remove(0);
                        }
                    }
                }
            }
        }
    }

    public void sendEvent(ProcessEvent event){
        try {
            if(!event.isAcknowledged()){
                // increment LC of the process while sending an event
                logicalClock_process += 1;
            }
            event.setLogical_time(logicalClock_process);
            Socket socket;
            ObjectOutputStream obj_op;
            // Multicast to all the processes, including itself
            for(int port : process_portList){
                socket = new Socket(server_host_addr,port);
                obj_op = new ObjectOutputStream(socket.getOutputStream());
                obj_op.writeObject(event);

                obj_op.close();
                socket.close();
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public boolean checkWhetherWeCanSendAckOfThisEvent(ProcessEvent event){
        // if acknowledgement is already sent to a process
        if(!ack_buffer.isEmpty() && ack_buffer.containsKey(event.getEvent_id())){
            if(ack_buffer.get(event.getEvent_id()).contains(process_id)){
                return false;
            }
        }
        // send acknowledgment to the event receiving process
        if(event.getPid() == pid){
            return true;
        }

        // send acknowledgement to the event sending process
        if(ack_buffer.containsKey(current_eventID) && ack_buffer.get(current_eventID).size() == num_processes){
            return true;
        }

        // Enforce total order
        if(logicalClock_process == event.getLogical_time()){
            if(pid > event.getPid()){
                return true;
            }
        }else if(logicalClock_process > event.getLogical_time()){
            return true;
        }

        return false;
    }

    public void deliverEvent(ProcessEvent event){
        // deliver  received event
        System.out.println(process_id + ": " + event.getProcess_id() + "." + event.getEvent_id());
        events_delivered += 1;
    }

 }
