import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/*
 * Class to store data in a node in the DHT ring.
 * 
 * Constructor:
 * - Data_Store(): Initializes the hashmap to store data.
 * 
 * Methods:
 * - insert(key, value): Inserts a key-value pair into the hashmap.
 * - delete(key): Deletes the given key from the hashmap.
 * - search(key): Searches for the given key in the hashmap.
 */
class Data_Store {

    // Key-Value Data
    public Map<String, String> data;

    /**
     * Initializes Data_Store with an empty HashMap to store data.
     */
    public Data_Store() {
        this.data = new HashMap<>();
    }

    /**
     * Inserts a key-value pair into the data store.
     *
     * @param key   The key to insert.
     * @param value The value associated with the key.
     */
    public void insert(String key, String value) {
        data.put(key, value);
    }

    /**
     * Deletes the given key from the data store.
     *
     * @param key The key to delete.
     */
    public void delete(String key) {
        data.remove(key);
    }

    /**
     * Searches for the given key in the data store and returns its value if found.
     * If not found, prints "Not Found" and returns null.
     *
     * @param searchKey The key to search for.
     * @return The value associated with the search key, or null if not found.
     */
    public String search(String searchKey) {
        if (data.containsKey(searchKey)) {
            return data.get(searchKey);
        } else {
            System.out.println("Not Found");
            System.out.println(data);
            return null;
        }
    }

}

/*
 * Class representing actual information about the Node in the DHT Ring, stores
 * IP and port of a node.
 * 
 * Constructor:
 * - Node_Info(): Initializes IP and port of the Node.
 * 
 * Methods:
 * - toString(): An overridden function of the Object class, converts the object
 * to a string.
 */
class Node_Info {

    // Declare IP address and Port Number
    public String ip;
    public int port;

    /**
     * Initializes Node_Info with the given IP address and port number.
     *
     * @param ip   The IP address of the node.
     * @param port The port number of the node.
     */
    public Node_Info(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    /**
     * Combines the IP address and the port number as a string.
     *
     * @return A string representation of the IP address and port number.
     */
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append(this.ip).append("|").append(this.port);
        return buffer.toString();
    }

}

/*
 * Represents a pair containing an entry and a node in the Finger Table.
 * 
 * Constructor:
 * - EntryNodePair(entry, node): Constructs an EntryNodePair with the specified
 * entry and node.
 * 
 * Methods:
 * - getEntry(): Returns the entry in the Finger Table.
 * - getNode(): Returns the corresponding node.
 * - setNode(newNode): Sets the node to the specified new node.
 */
class EntryNodePair {

    public int entry; // The entry in the Finger Table
    public Node node; // The corresponding node

    /**
     * Constructs an EntryNodePair with the specified entry and node.
     *
     * @param entry The entry in the Finger Table.
     * @param node  The corresponding node.
     */
    public EntryNodePair(int entry, Node node) {
        this.entry = entry;
        this.node = node;
    }

    /**
     * Returns the entry in the Finger Table.
     *
     * @return The entry in the Finger Table.
     */
    public int getEntry() {
        return this.entry;
    }

    /**
     * Returns the corresponding node.
     *
     * @return The corresponding node.
     */
    public Node getNode() {
        return this.node;
    }

    /**
     * Sets the node to the specified new node.
     *
     * @param newNode The new node to set.
     */
    public void setNode(Node newNode) {
        this.node = newNode;
    }
}

/*
 * Class to represent the finger table of a node.
 * 
 * Constructor:
 * - Finger_Table(myID): Initializes every entry of the finger table with -1 as
 * its successor.
 * 
 * Methods:
 * - print(): Prints every entry and its successor for the node.
 */
class Finger_Table {

    public ArrayList<EntryNodePair> table;
    public static final int M = 8;

    /**
     * Initializes a Finger Table for a node.
     *
     * @param myId The ID of the node.
     */
    public Finger_Table(int myId) {
        this.table = new ArrayList<>();

        for (int i = 0; i < M; i++) {
            int x = (int) Math.pow(2, i);
            int entry = (myId + x) % (int) Math.pow(2, M);
            Node init_node = null;
            EntryNodePair pair = new EntryNodePair(entry, init_node);
            this.table.add(pair);
        }
    }

    /**
     * Prints the contents of the Finger Table.
     */
    public void print() {
        for (int index = 0; index < table.size(); index++) {
            EntryNodePair pair = table.get(index);
            if (pair.getNode() == null) {
                System.out.println("Entry: " + index + " Interval start: " + pair.getEntry() + " Successor: None");
            } else {
                System.out.println("Entry: " + index + " Interval start: " + pair.getEntry() + " Successor: "
                        + pair.getNode().getId());
            }
        }
    }

    /**
     * Returns a string containing information about the fingers of a node.
     * 
     * @return A string containing finger table information.
     */
    public String getFingerTableInfo() {
        StringBuilder info = new StringBuilder();
        for (int index = 0; index < table.size(); index++) {
            EntryNodePair pair = table.get(index);
            if (pair.getNode() == null) {
                info.append("Entry: ").append(index).append(" Interval start: ").append(pair.getEntry())
                        .append(" Successor: None\n");
            } else {
                info.append("Entry: ").append(index).append(" Interval start: ").append(pair.getEntry())
                        .append(" Successor: ").append(pair.getNode().getId()).append("\n");
            }
        }
        return info.toString();
    }

}

/*
 * Class to handle requests and responsible for sending messages over a network
 * using a Socket.
 * 
 * Methods:
 * - sendMessage(ip, port, message): Sends a message to the provided address.
 * 
 * Example:
 * - Message Sent: "join_request|<node_id>" (Joining node sends a request to an
 * existing node to join the ring)
 * - Response Received:
 * "successor_info|<successor_id>|<successor_ip>|<successor_port>"
 */
class Request_Handler {

    // Suppressing resource warnings since Socket and Scanner need to be closed
    // properly
    @SuppressWarnings("resource")

    /**
     * Sends a message to a specified IP address and port using a Socket for
     * communication.
     *
     * @param ip      The IP address of the destination.
     * @param port    The port number of the destination.
     * @param message The message to be sent.
     * @return The response received from the destination, or an empty string if an
     *         error occurs or no response is received.
     */
    public String sendMessage(String ip, int port, String message) {
        try (Socket socket = new Socket(ip, port)) {
            // Try-with-resources block for proper resource management

            // Obtaining the output stream from the socket
            OutputStream output = socket.getOutputStream();
            // Creating a PrintWriter for writing text to the output stream
            PrintWriter writer = new PrintWriter(output, true);

            // Sending the message to the output stream
            writer.println(message);

            // Obtaining the input stream from the socket
            InputStream input = socket.getInputStream();
            // Creating a Scanner for reading text from the input stream
            Scanner scanner = new Scanner(input);
            // Checking if there is another line in the input stream
            if (scanner.hasNextLine()) {
                // Returning the next line if available
                return scanner.nextLine();
            }
        } catch (IOException e) {
            // Handling IOException by printing the stack trace
            e.printStackTrace();
        }

        // If an exception occurs or no line is received, return an empty string
        return "";
    }

}

/*
 * Class to represent a node in the Chord DHT network.
 * 
 * Constructor:
 * - Node(ip, port): Initializes a new node with the given IP address and port.
 * 
 * Methods:
 * - hash(message): Calculates the hash value of a message using SHA-256
 * algorithm.
 * - processRequest(message): Processes incoming request messages and performs
 * corresponding operations.
 * - serveRequests(Socket conn, SocketAddress addr): Serves incoming requests
 * from a client socket.
 * - start(): Starts the Chord DHT node by initializing a server socket and
 * continuously accepting incoming connections.
 * - insertKey(key, value): Inserts a key-value pair into the Chord DHT network.
 * - deleteKey(key): Deletes a key from the Chord DHT network.
 * - searchKey(key): Searches for the value corresponding to a given key in the
 * Chord DHT network.
 * - joinRequestFromOtherNode(nodeId): Handles a join request from another node
 * by finding its successor.
 * - join(nodeIp, nodePort): Joins the Chord DHT network by connecting to an
 * existing node.
 * - findPredecessor(searchId): Finds the predecessor node responsible for a
 * given key.
 * - findSuccessor(searchId): Finds the successor node responsible for a given
 * key.
 * - closestPrecedingNode(searchId): Finds the closest preceding node to a given
 * key.
 * - sendKeys(idOfJoiningNode): Sends keys to a joining node.
 * - stabilize(): Periodically stabilizes the Chord DHT network by updating
 * successor and predecessor information.
 * - notify(nodeId, nodeIp, nodePort): Notifies the node about a new predecessor
 * in the network.
 * - fixFingers(): Fixes fingers in the finger table by updating entries with
 * successors found in the network.
 * - getSuccessor(): Returns the successor node's information.
 * - getPredecessor(): Returns the predecessor node's information.
 * - getId(): Returns the ID of the node as a string.
 * - getIpPort(stringFormat): Extracts IP and port from the string format of
 * node information.
 * - getBackwardDistance(nodeId): Calculates the backward distance between the
 * node and another node.
 * - getBackwardDistance2Nodes(nodeId2, nodeId1): Calculates the backward
 * distance between two nodes.
 * - getForwardDistance(nodeId): Calculates the forward distance between the
 * node and another node.
 * - getForwardDistance2Nodes(node2, node1): Calculates the forward distance
 * between two nodes.
 */
public class Node {

    public String ip;
    public int port;
    public Node_Info nodeInfo;
    public int id;
    public Node predecessor;
    public Node successor;
    public Finger_Table fingerTable;
    public Data_Store dataStore;
    public Request_Handler requestHandler;

    /**
     * Initializes a Node with the given IP address and port.
     * 
     * @param ip   The IP address of the node.
     * @param port The port of the node.
     */
    public Node(String ip, int port) {
        // Set the IP address and port of the node
        this.ip = ip;
        this.port = port;

        // Create a Node_Info object for the node
        this.nodeInfo = new Node_Info(ip, port);

        // Calculate the hash value of the Node_Info object to get the ID of the node
        this.id = hash(String.valueOf(this.nodeInfo));

        // Set the predecessor and successor of the node to null initially
        this.predecessor = null;
        this.successor = null;

        // Initialize the Finger Table of the node using its ID
        this.fingerTable = new Finger_Table(this.id);

        // Initialize the Data Store of the node
        this.dataStore = new Data_Store();

        // Initialize the Request Handler of the node
        this.requestHandler = new Request_Handler();
    }

    /**
     * Calculates the hash value of a message using the SHA-256 algorithm.
     * 
     * @param message The message for which the hash value is to be calculated.
     * @return The hash value of the message.
     */
    public int hash(String message) {
        try {
            // Create a MessageDigest instance using the SHA-256 algorithm
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            // Compute the hash value of the message
            byte[] digest = md.digest(message.getBytes());

            // Convert the byte array to an integer and handle sign properly
            ByteBuffer buffer = ByteBuffer.wrap(digest);
            return Math.abs(buffer.getInt()) % (int) Math.pow(2, Finger_Table.M);
        } catch (NoSuchAlgorithmException e) {
            // Handle NoSuchAlgorithmException by printing stack trace and returning -1
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * Processes the incoming request message and performs the corresponding
     * operation.
     * 
     * @param message The incoming request message.
     * @return The result of the operation.
     */
    public String processRequest(String message) {
        // Splitting the message to extract operation and arguments
        String[] parts = message.split("\\|");
        // Extracting the operation from the message
        String operation = parts[0];
        // Extracting arguments from the message
        String[] args = new String[parts.length - 1];
        for (int i = 1; i < parts.length; i++) {
            args[i - 1] = parts[i];
        }

        String result = "Done";

        // Switch case to handle different operations based on the message
        switch (operation) {

            case "Insert_Server": // If the operation is to insert data in the server
                String[] data = parts[1].split(":"); // Splitting data into key-value pair
                String key = data[0]; // Extracting key
                String value = data[1]; // Extracting value
                this.dataStore.insert(key, value); // Inserting key-value pair into data store
                result = "Inserted";
                break;

            case "Delete_Server": // If the operation is to delete data from the server
                String dataToDelete = parts[1]; // Extracting data to delete
                this.dataStore.delete(dataToDelete); // Deleting data from data store
                result = "Deleted";
                break;

            case "Search_Server": // If the operation is to search data in the server
                String searchData = parts[1]; // Extracting data to search
                if (this.dataStore.data.containsKey(searchData)) { // Checking if data exists in data store
                    return dataStore.data.get(searchData); // Returning data if found
                } else {
                    return "NOT FOUND";
                }

            case "Send_Keys": // If the operation is to send keys to a joining node
                int idOfJoiningNode = Integer.parseInt(args[0]); // Extracting joining node's ID
                result = sendKeys(idOfJoiningNode);
                break;

            case "Insert": // If the operation is to insert data
                String[] insertData = parts[1].split(":"); // Splitting data into key-value pair
                String insertKey = insertData[0]; // Extracting key
                String insertValue = insertData[1]; // Extracting value
                result = this.insertKey(insertKey, insertValue);
                break;

            case "Delete": // If the operation is to delete data
                String deleteKey = parts[1]; // Extracting key to delete
                result = this.deleteKey(deleteKey);
                break;

            case "Search": // If the operation is to search data
                String searchKey = parts[1]; // Extracting key to search
                result = this.searchKey(searchKey);
                break;

            case "Join_Request": // If the operation is a join request from another node
                int nodeId = Integer.parseInt(args[0]); // Extracting ID of the joining node
                result = this.joinRequestFromOtherNode(nodeId);
                break;

            case "Find_Predecessor": // If the operation is to find predecessor node
                int searchID = Integer.parseInt(args[0]); // Extracting ID to search
                result = this.findPredecessor(searchID);
                break;

            case "Find_Successor": // If the operation is to find successor node
                searchID = Integer.parseInt(args[0]); // Extracting ID to search
                result = this.findSuccessor(searchID);
                break;

            case "Get_Successor": // If the operation is to get successor node
                result = this.getSuccessor();
                break;

            case "Get_Predecessor": // If the operation is to get predecessor node
                result = this.getPredecessor();
                break;

            case "Get_Id": // If the operation is to get node ID
                result = this.getId();
                break;

            case "Get_Finger_Table": // If the operation is to get finger table of the node (using GUI)
                result = this.fingerTable.getFingerTableInfo();
                break;

            case "Get_Data_Store": // If the operation is to get data store of the node (using GUI)
                result = this.dataStore.data.toString();
                break;

            case "Get_Info":
                result =  this.ip + "/" + String.valueOf(this.port) + "/" + String.valueOf(this.id) + "/" + String.valueOf(this.predecessor.id) + "/" + String.valueOf(this.successor.id);
                break;

            case "Notify": // If the operation is to notify a node
                int nodeID = Integer.parseInt(args[0]); // Extracting ID of the node to notify
                String nodeIP = args[1]; // Extracting IP of the node to notify
                int nodePort = Integer.parseInt(args[2]); // Extracting port of the node to notify
                this.notify(nodeID, nodeIP, nodePort); // Notifying the node
                break;

            default:
                break;
        }

        return result;
    }

    /**
     * Serves incoming requests from a client socket.
     * 
     * @param conn The socket connection with the client.
     * @param addr The address of the remote socket.
     */
    public void serveRequests(Socket conn, SocketAddress addr) {
        try {
            // Reading the incoming message as UTF-encoded string
            @SuppressWarnings("resource")
            Scanner scanner = new Scanner(conn.getInputStream(), "UTF-8");
            String data = scanner.nextLine().trim();

            // Processing the request and getting the result
            String result = this.processRequest(data);
            // Creating a PrintWriter for writing text to the output stream
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"), true);
            // Writing the result to the output stream
            writer.println(result);
        } catch (IOException e) {
            System.err.println("Error occured while serving request: ");
            e.printStackTrace(); // Handling IOException if any
        } finally {
            try {
                conn.close(); // Closing the connection
            } catch (IOException e) {
                e.printStackTrace(); // Handling IOException if any while closing connection
            }
        }
    }

    /**
     * Starts the Chord DHT node by initializing a server socket and continuously
     * accepting incoming connections.
     * 
     * The method starts two separate threads:
     * 1. One thread for stabilizing the node periodically.
     * 2. Another thread for fixing the fingers of the node.
     */
    public void start() {
        // Create a server socket bound to the specific address and port
        try (ServerSocket serverSocket = new ServerSocket(nodeInfo.port, 50, InetAddress.getByName(nodeInfo.ip))) {

            serverSocket.setReuseAddress(true);
            // Starting a new thread for stabilizing the node
            Thread threadForStabilize = new Thread(() -> stabilize());
            threadForStabilize.start();

            // Starting a new thread for fixing fingers of the node
            Thread threadForFixFinger = new Thread(() -> fixFingers());
            threadForFixFinger.start();

            // Continuously accepting incoming connections
            while (true) {
                // Accepting a new connection from the server socket
                Socket socket = serverSocket.accept();

                // Getting the address of the remote socket
                SocketAddress addr = socket.getRemoteSocketAddress();

                // Starting a new thread to serve the incoming request
                Thread t = new Thread(() -> serveRequests(socket, addr));
                t.start();
            }
        } catch (IOException e) {
            // Handling IOException if any
            e.printStackTrace();
        }
    }

    /**
     * Inserts a key-value pair into the Chord DHT network.
     * 
     * @param key   The key to insert.
     * @param value The corresponding value to insert.
     * @return A message indicating the result of the insertion operation.
     */
    public String insertKey(String key, String value) {
        try {
            // Calculate the hash value of the key
            int idOfKey = this.hash(key);

            // Find the successor node responsible for the key's hash value
            String succ = this.findSuccessor(idOfKey);

            // Extract IP address and port number of the successor node
            String[] ipPort = this.getIpPort(succ);
            // Error Handling for invalid ip and port
            if (ipPort == null || ipPort.length < 2 || (ipPort[0] == "Invalid IP" && ipPort[1] == "Invalid Port")) {
                throw new RuntimeException("Invalid Port information received");
            }
            String ip = ipPort[0];
            int port = Integer.parseInt(ipPort[1]);

            // Send a message to the successor node to insert the key-value pair
            this.requestHandler.sendMessage(ip, port, "Insert_Server|" + key + ":" + value);

            // Return a message indicating successful insertion
            return "Inserted at node id " + new Node(ip, port).id + " key was " + key + " key hash was " + idOfKey;
        } catch (NumberFormatException e) {
            // Handle any number format exceptions (e.g., parsing port number)
            e.printStackTrace();
            return "Error inserting key: " + e.getMessage();
        } catch (RuntimeException e) {
            e.printStackTrace();
            return "Error inserting key: " + e.getMessage();
        }
    }

    /**
     * Deletes the key from the DHT.
     * 
     * @param key The key to be deleted.
     * @return A message indicating the success of the deletion.
     */
    public String deleteKey(String key) {
        try {// Calculate the hash value of the key
            int idOfKey = hash(key);
            // Find the successor node responsible for the key
            String succ = findSuccessor(idOfKey);
            // Get the IP address and port of the successor node
            String[] ipPort = getIpPort(succ);
            // Error Handling for invalid ip and port
            if (ipPort == null || ipPort.length < 2 || (ipPort[0] == "Invalid IP" && ipPort[1] == "Invalid Port")) {
                throw new RuntimeException("Invalid Port information received");
            }
            // Send a delete request to the successor node
            requestHandler.sendMessage(ipPort[0], Integer.parseInt(ipPort[1]), "Delete_Server|" + key);
            // Return a message confirming the deletion
            return "deleted at node id " + (new Node(ipPort[0], Integer.parseInt(ipPort[1]))).id + " key was " + key
                    + " key hash was " + idOfKey;
        } catch (RuntimeException e) {
            e.printStackTrace();
            return "Error inserting key: " + e.getMessage();
        }
    }

    /**
     * Searches for the value corresponding to the given key in the DHT.
     * 
     * @param key The key to be searched.
     * @return The value associated with the key, or a message indicating that the
     *         key was not found.
     */
    public String searchKey(String key) {
        try {// Calculate the hash value of the key
            int idOfKey = hash(key);
            // Find the successor node responsible for the key
            String succ = findSuccessor(idOfKey);
            // Get the IP address and port of the successor node
            String[] ipPort = getIpPort(succ);
            // Error Handling for invalid ip and port
            if (ipPort == null || ipPort.length < 2 || (ipPort[0] == "Invalid IP" && ipPort[1] == "Invalid Port")) {
                throw new RuntimeException("Invalid Port information received");
            }
            // Send a search request to the successor node and get the response
            String data = requestHandler.sendMessage(ipPort[0], Integer.parseInt(ipPort[1]), "Search_Server|" + key);
            // Return the data received from the successor node
            return data;
        } catch (RuntimeException e) {
            e.printStackTrace();
            return "Error inserting key: " + e.getMessage();
        }
    }

    /**
     * Handles a join request from another node by finding its successor.
     * 
     * @param nodeId The ID of the joining node.
     * @return The successor node of the joining node.
     */
    public String joinRequestFromOtherNode(int nodeId) {
        // Find the successor node of the joining node
        return this.findSuccessor(nodeId);
    }

    /**
     * Joins the Chord network by connecting to an existing node.
     * 
     * @param nodeIp   The IP address of the existing node.
     * @param nodePort The port of the existing node.
     */
    public void join(String nodeIp, int nodePort) {
        try {
            // Check if the requestHandler is initialized
            if (requestHandler == null) {
                throw new NullPointerException("requestHandler is not initialized");
            }
            // Prepare the message to send for joining
            String data = "Join_Request|" + this.id;
            // Send the join request to the existing node and get the successor information
            String succ = requestHandler.sendMessage(nodeIp, nodePort, data);
            // Check if successor information is received successfully
            if (succ == null || succ.isEmpty() || succ == "None") {
                throw new RuntimeException("Failed to get successor information");
            }

            // Extract the IP address and port of the successor node
            String[] ipPort = getIpPort(succ);
            // Error Handling for invalid ip and port
            if (ipPort == null || ipPort.length < 2 || (ipPort[0] == "Invalid IP" && ipPort[1] == "Invalid Port")) {
                throw new RuntimeException("Invalid Port information received");
            }

            // Set the successor node and update the finger table
            this.successor = new Node(ipPort[0], Integer.parseInt(ipPort[1]));
            this.fingerTable.table.get(0).setNode(this.successor);
            this.predecessor = null;

            if (this.successor.id != this.id) {
                // Retrieve keys from the successor node
                data = this.requestHandler.sendMessage(this.successor.ip, this.successor.port, "Send_Keys|" + this.id);
                if (data == null || data.isEmpty()) {
                    System.err.println("No keys received from the successor");
                } else {
                    // Parse and store the received key-value pairs
                    String[] key_values = data.split(":");
                    for (String key_value : key_values) {
                        if (key_value.length() > 1) {
                            String[] parts = key_value.split("\\|");
                            if (parts.length >= 2) {
                                this.dataStore.data.put(parts[0], parts[1]);
                            } else {
                                System.err.println("Invalid key-value pair: " + key_value);
                            }
                        }
                    }
                }
            }
        } catch (NullPointerException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } catch (RuntimeException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Finds the predecessor node responsible for the given key.
     * 
     * @param searchId The ID of the key for which the predecessor node is searched.
     * @return The information about the predecessor node.
     */
    public String findPredecessor(int searchId) {
        try {
            // If the current node is the predecessor
            if (searchId == this.id) {
                return this.nodeInfo.toString();
            }
            // If the current node is the only node in the network
            if (this.predecessor != null && this.successor.id == this.id) {
                return this.nodeInfo.toString();
            }
            // If the successor of the current node is the closest node to the key
            if (this.successor != null
                    && this.getForwardDistance(this.successor.id) > this.getForwardDistance(searchId)) {
                return this.nodeInfo.toString();
            } else {
                // Otherwise, recursively find the predecessor node on the routing path
                Node newHopNode = this.closestPrecedingNode(searchId);
                if (newHopNode == null) {
                    return "None";
                }
                // Extract IP and port of the new hop node
                String[] ipPort = this.getIpPort(newHopNode.nodeInfo.toString());
                // Error Handling for invalid ip and port
                if (ipPort == null || ipPort.length < 2 || (ipPort[0] == "Invalid IP" && ipPort[1] == "Invalid Port")) {
                    throw new RuntimeException("Invalid Port information received");
                }
                // If the new hop node is the current node, return its information
                if (ipPort[0].equals(this.ip) && Integer.parseInt(ipPort[1]) == this.port) {
                    return this.nodeInfo.toString();
                }
                // Send request to the new hop node to find the predecessor node
                String data = requestHandler.sendMessage(ipPort[0], Integer.parseInt(ipPort[1]),
                        "Find_Predecessor|" + searchId);
                return data;
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            return "Error inserting key: " + e.getMessage();
        } catch (Exception e) {
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
            return "Error";
        }
    }

    /**
     * Finds the successor node responsible for the given key.
     * 
     * @param searchId The ID of the key for which the successor node is searched.
     * @return The information about the successor node.
     */
    public String findSuccessor(int searchId) {
        try {
            // If the current node is responsible for the key, return its information
            if (searchId == this.id) {
                return this.nodeInfo.toString();
            }
            // Find the predecessor node for the given key
            String predecessor = this.findPredecessor(searchId);
            // If no predecessor node found, return "None"
            if (predecessor.equals("None")) {
                return "None";
            } else {
                // Extract IP and port of the predecessor node
                String[] ipPort = this.getIpPort(predecessor);
                // Error Handling for invalid ip and port
                if (ipPort == null || ipPort.length < 2 || (ipPort[0] == "Invalid IP" && ipPort[1] == "Invalid Port")) {
                    throw new RuntimeException("Invalid Port information received");
                }
                // If valid IP and port are retrieved, request successor information from the
                // predecessor
                if (ipPort.length < 2) {
                    return "Invalid predecessor information";
                }
                // Send request to the predecessor node to get its successor
                String data = requestHandler.sendMessage(ipPort[0], Integer.parseInt(ipPort[1]), "Get_Successor");
                return data;
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            return "Error inserting key: " + e.getMessage();
        } catch (Exception e) {
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
            return "Error";
        }
    }

    /**
     * Finds the closest preceding node to the given key.
     * 
     * @param searchId The ID of the key for which the closest preceding node is
     *                 searched.
     * @return The closest preceding node to the given key.
     */
    public Node closestPrecedingNode(int searchId) {
        Node closestNode = null;
        int minDistance = (int) Math.pow(2, Finger_Table.M) + 1;

        try {
            // Iterate through the finger table entries to find the closest preceding node
            for (int i = Finger_Table.M - 1; i >= 0; i--) {
                EntryNodePair en_pair = this.fingerTable.table.get(i);
                Node node = en_pair.getNode();

                // Update the closest node if the current node is closer to the key
                if (node != null && getForwardDistance2Nodes(node.id, searchId) < minDistance) {
                    closestNode = node;
                    minDistance = getForwardDistance2Nodes(node.id, searchId);
                }
            }
        } catch (Exception e) {
            // Handle any exceptions that occur during the iteration
            e.printStackTrace();
        }

        return closestNode;
    }

    /**
     * Sends keys to a joining node and removes them from the current node's data
     * store.
     * 
     * @param idOfJoiningNode The ID of the node joining the network.
     * @return A string containing the keys and their corresponding values to be
     *         sent to the joining node.
     */
    public String sendKeys(int idOfJoiningNode) {
        String data = "";
        ArrayList<String> keysToBeRemoved = new ArrayList<>();
        // Iterate through the keys in the current node's data store
        for (String key : this.dataStore.data.keySet()) {
            int keyId = hash(key);
            // Check if the key should be transferred to the joining node
            if (getForwardDistance2Nodes(keyId, idOfJoiningNode) < getForwardDistance2Nodes(keyId, this.id)) {
                // Append the key and its value to the data string
                data += key + "|" + this.dataStore.data.get(key) + ":";
                // Add the key to the list of keys to be removed from the current node's data
                // store
                keysToBeRemoved.add(key);
            }
        }
        // Remove the keys that are being transferred to the joining node from the
        // current node's data store
        for (String key : keysToBeRemoved) {
            this.dataStore.data.remove(key);
        }
        return data;
    }

    /**
     * Periodically stabilizes the Chord DHT network by updating successor and
     * predecessor information.
     */
    public void stabilize() {
        while (true) {
            if (this.successor == null) {
                try {
                    Thread.sleep(10000); // Wait for 10 seconds if the successor is not available
                    continue;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            String data = "Get_Predecessor"; // Prepare a message to get the predecessor of the successor node
            if (this.successor.ip.equals(this.ip) && this.successor.port == this.port) {
                try {
                    Thread.sleep(10000); // If the successor is the current node, wait for 10 seconds
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // Get the predecessor of the successor
            String result = this.requestHandler.sendMessage(this.successor.ip, this.successor.port, data);
            if (result.equals("None") || result.length() == 0 || result == null) { // If no predecessor found, notify
                                                                                   // the successor
                this.requestHandler.sendMessage(this.successor.ip, this.successor.port,
                        "Notify|" + this.id + "|" + this.nodeInfo.toString());
                continue;
            }
            String[] ipPort = {};
            try {
                ipPort = getIpPort(result); // Extract IP and port of the predecessor
                // Error Handling for invalid ip and port
                if (ipPort == null || ipPort.length < 2 || (ipPort[0] == "Invalid IP" && ipPort[1] == "Invalid Port")) {
                    throw new RuntimeException("Invalid Port information received");
                }
            } catch (RuntimeException e) {
                System.err.println("Error getting IP and port: " + e.getMessage());
                e.printStackTrace();
            }
            // Get the ID of the predecessor
            int resultId = Integer
                    .parseInt(this.requestHandler.sendMessage(ipPort[0], Integer.parseInt(ipPort[1]), "Get_Id"));
            // If the predecessor is closer to the current node
            if (getBackwardDistance(resultId) > getBackwardDistance(this.successor.id)) {
                // Update the successor
                this.successor = new Node(ipPort[0], Integer.parseInt(ipPort[1]));
                // Update the first entry in the finger table
                this.fingerTable.table.get(0).setNode(this.successor);
            }
            // Notify the successor about the current node
            this.requestHandler.sendMessage(this.successor.ip, this.successor.port,
                    "Notify|" + this.id + "|" + this.nodeInfo.toString());
            // Print network status for debugging
            System.out.println("============================================================================");
            System.out.println("STABILIZING");
            System.out.println("============================================================================");
            System.out.println("ID: " + this.id);
            if (this.successor != null) {
                System.out.println("Successor ID: " + this.successor.id);
            }
            if (this.predecessor != null) {
                System.out.println("Predecessor ID: " + this.predecessor.id);
            }
            System.out.println("============================================================================");
            System.out.println("--------------------------------FINGER TABLE--------------------------------");
            this.fingerTable.print();
            System.out.println("============================================================================");
            System.out.println("DATA STORE");
            System.out.println("============================================================================");
            System.out.println(this.dataStore.data.toString());
            System.out.println("============================================================================");
            System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX END XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
            System.out.println();
            System.out.println();
            System.out.println();
            try {
                Thread.sleep(10000); // Wait for 10 seconds before next stabilization
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Notifies the node about a new predecessor in the network.
     * If the new node is closer to the current node than the existing predecessor,
     * update the predecessor.
     * 
     * @param nodeId   The ID of the new predecessor node.
     * @param nodeIp   The IP address of the new predecessor node.
     * @param nodePort The port of the new predecessor node.
     */
    public void notify(int nodeId, String nodeIp, int nodePort) {
        // If predecessor exists and new node is closer, update predecessor
        if (this.predecessor != null) {
            if (getBackwardDistance(nodeId) < getBackwardDistance(Integer.parseInt(predecessor.getId()))) {
                this.predecessor = new Node(nodeIp, nodePort);
                return;
            }
        }
        // If no predecessor exists or new node is closer or falls between current node
        // and predecessor,
        // update predecessor and successor (if necessary)
        if (this.predecessor == null || this.findPredecessor(this.id).equals("None")
                || (nodeId > Integer.parseInt(predecessor.getId()) && nodeId < id)
                || (id == Integer.parseInt(predecessor.getId()) && nodeId != id)) {
            this.predecessor = new Node(nodeIp, nodePort);
            if (id == Integer.parseInt(successor.getId())) {
                successor = new Node(nodeIp, nodePort);
                fingerTable.table.get(0).setNode(successor);
            }
        }
    }

    /**
     * Fixes fingers in the finger table by updating entries with successors found
     * in the network.
     */
    public void fixFingers() {
        try {
            Random rand = new Random();
            while (true) {
                // Select a random index in the finger table to fix
                int randomIndex = rand.nextInt(Finger_Table.M - 1) + 1;
                int finger = fingerTable.table.get(randomIndex).getEntry(); // Get the finger value
                String data = findSuccessor(finger); // Find the successor for the finger
                if (data.equals("") || data.equals("None") || data == null) { // If no successor found, wait for 10
                                                                              // seconds and continue
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                String[] ipPort = {};
                try {
                    ipPort = getIpPort(data); // Extract IP and port of the successor
                    // Error Handling for invalid ip and port
                    if (ipPort == null || ipPort.length < 2
                            || (ipPort[0] == "Invalid IP" && ipPort[1] == "Invalid Port")) {
                        throw new RuntimeException("Invalid Port information received");
                    }
                } catch (RuntimeException e) {
                    System.err.println("Error getting IP and port: " + e.getMessage());
                    e.printStackTrace();
                }
                // Update the finger table entry with the new successor
                fingerTable.table.get(randomIndex).setNode(new Node(ipPort[0], Integer.parseInt(ipPort[1])));
                try {
                    Thread.sleep(10000); // Wait for 10 seconds before fixing next finger
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Returns the successor node's information.
     * If the successor does not exist, returns "None".
     * 
     * @return A string representation of the successor node's information.
     */
    public String getSuccessor() {
        if (successor == null) {
            return "None";
        }
        return successor.nodeInfo.toString();
    }

    /**
     * Returns the predecessor node's information.
     * If the predecessor does not exist, returns "None".
     * 
     * @return A string representation of the predecessor node's information.
     */
    public String getPredecessor() {
        if (predecessor == null) {
            return "None";
        }
        return predecessor.nodeInfo.toString();
    }

    /**
     * Returns the node's ID as a string.
     * 
     * @return The node's ID as a string.
     */
    public String getId() {
        return String.valueOf(this.id);
    }

    /**
     * Extracts IP and port from the string format of node information.
     * 
     * @param stringFormat The string format of node information (e.g., "IP|Port").
     * @return An array containing IP and port extracted from the input string.
     */
    public String[] getIpPort(String stringFormat) {
        String[] parts = stringFormat.trim().split("\\|");
        if (parts.length >= 2) {
            return new String[] { parts[0], parts[1] };
        } else {
            return new String[] { "Invalid IP", "Invalid Port" };
        }
    }

    /**
     * Calculates the backward distance between the current node and another node.
     * 
     * @param nodeId The ID of the other node.
     * @return The backward distance from the current node to the specified node.
     */
    public int getBackwardDistance(int nodeId) {
        int distance;
        if (this.id > nodeId) {
            distance = this.id - nodeId;
        } else if (this.id == nodeId) {
            distance = 0;
        } else {
            distance = (int) (Math.pow(2, Finger_Table.M) - Math.abs(this.id - nodeId));
        }
        return Math.abs(distance);
    }

    /**
     * Calculates the backward distance between two nodes.
     * 
     * @param nodeId2 The ID of the second node.
     * @param nodeId1 The ID of the first node.
     * @return The backward distance from the second node to the first node.
     */
    public int getBackwardDistance2Nodes(int nodeId2, int nodeId1) {
        int distance;
        if (nodeId2 > nodeId1) {
            distance = nodeId2 - nodeId1;
        } else if (nodeId2 == nodeId1) {
            distance = 0;
        } else {
            distance = (int) (Math.pow(2, Finger_Table.M) - Math.abs(nodeId2 - nodeId1));
        }
        return Math.abs(distance);
    }

    /**
     * Calculates the forward distance from the current node to another node.
     * 
     * @param nodeId The ID of the other node.
     * @return The forward distance from the current node to the specified node.
     */
    public int getForwardDistance(int nodeId) {
        return (int) Math.abs((Math.pow(2, Finger_Table.M) - getBackwardDistance(nodeId)));
    }

    /**
     * Calculates the forward distance between two nodes.
     * 
     * @param node2 The ID of the second node.
     * @param node1 The ID of the first node.
     * @return The forward distance from the second node to the first node.
     */
    public int getForwardDistance2Nodes(int node2, int node1) {
        return (int) Math.abs((Math.pow(2, Finger_Table.M) - getBackwardDistance2Nodes(node2, node1)));
    }

    public static void main(String[] args) {

        String ip = "192.168.240.106";

        if (args.length == 2) {
            // Joining an existing ring
            System.out.println("Joining Ring");
            Node node = new Node(ip, Integer.parseInt(args[0]));
            node.join(ip, Integer.parseInt(args[1]));
            node.start();
        } else if (args.length == 1) {
            // Creating a new ring
            System.out.println("CREATING RING: ");
            Node node = new Node(ip, Integer.parseInt(args[0]));
            // Set predecessor and successor for the first node
            node.predecessor = new Node(ip, Integer.parseInt(args[0]));
            node.successor = new Node(ip, Integer.parseInt(args[0]));
            // Update finger table
            node.fingerTable.table.get(0).setNode(node.successor);
            node.start();
        } else {
            System.out.println("Invalid arguments. Usage: java Main <port> [existing_node_port]");
        }
    }
}