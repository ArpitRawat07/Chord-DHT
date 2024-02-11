import javax.swing.*;
import java.awt.*;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

/**
 * The Chord_DHT_GUI class represents the graphical user interface (GUI) for a
 * Chord Distributed Hash Table (DHT).
 * It provides functionality to create, join, insert, search, and delete data in
 * the Chord DHT.
 * The GUI consists of various panels for different operations such as creating,
 * joining, and manipulating the DHT.
 */
public class Chord_DHT_GUI extends JFrame {
    public JTextField nodeIpField, nodePortField, showNodeIpField, showNodePortField, existingNodeIpField,
            existingNodePortField;
    public String currNodeIP, currNodePort;
    public Node curr_node;
    public JTextField currentNodeHashField, successorField, predecessorField, currentNodePortField, currentNodeIpField;

    public Chord_DHT_GUI() {
        currNodeIP = "None";
        currNodePort = "None";
        curr_node = null;
        setTitle("Chord DHT");
        setSize(1024, 700);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        // Create outer panel with BoxLayout
        JPanel outerPanel = new JPanel();
        outerPanel.setLayout(new BoxLayout(outerPanel, BoxLayout.X_AXIS));
        outerPanel.add(Box.createHorizontalStrut(2));

        // Create left column panel
        JPanel leftColumnPanel = new JPanel();
        leftColumnPanel.setLayout(new BoxLayout(leftColumnPanel, BoxLayout.Y_AXIS));
        leftColumnPanel.add(Box.createVerticalStrut(5)); // Add some spacing

        // Add "CurrentNodeInfo" panel to the right column
        JPanel currentInfoPanel = createCurrentInfoPanel();
        leftColumnPanel.add(currentInfoPanel);

        // Add "Create" panel to the left column
        JPanel createPanel = createCreatePanel();
        leftColumnPanel.add(createPanel);

        // Add "Show" panel to the left column
        JPanel showPanel = createShowPanel();
        leftColumnPanel.add(showPanel);

        // Add left column panel to the outer panel
        outerPanel.add(leftColumnPanel);

        // Add spacing before the separator
        outerPanel.add(Box.createHorizontalStrut(10));

        // Create separator to visually divide the columns
        JSeparator separator = new JSeparator(SwingConstants.VERTICAL);
        separator.setForeground(Color.DARK_GRAY); // Darken the color
        outerPanel.add(separator);

        // Add spacing after the separator
        outerPanel.add(Box.createHorizontalStrut(10));

        // Create right column panel
        JPanel rightColumnPanel = new JPanel();
        rightColumnPanel.setLayout(new BoxLayout(rightColumnPanel, BoxLayout.Y_AXIS));
        rightColumnPanel.add(Box.createVerticalStrut(5)); // Add some spacing

        // Add "Join" panel to the right column
        JPanel joinPanel = createJoinPanel();
        rightColumnPanel.add(joinPanel);

        // Add "Insertion" panel to the right column
        JPanel insertPanel = createInsertionPanel();
        rightColumnPanel.add(insertPanel);

        // Add "Search" panel to the right column
        JPanel searchPanel = createSearchPanel();
        rightColumnPanel.add(searchPanel);

        // Add "Deletion" panel to the right column
        JPanel deletePanel = createDeletionPanel();
        rightColumnPanel.add(deletePanel);

        // Add right column panel to the outer panel
        outerPanel.add(rightColumnPanel);

        // Add outer panel to the frame
        add(outerPanel);

        // Center the frame on the screen
        setLocationRelativeTo(null);

        setVisible(true);
    }

    /**
     * Creates a panel to display information about the current node in the Chord
     * DHT.
     * This panel includes fields for the current node's IP address, port number,
     * hash ID, successor, and predecessor.
     * The fields are displayed in a grid layout with titled borders for
     * organization.
     * 
     * @return JPanel representing the current node information panel.
     */
    private JPanel createCurrentInfoPanel() {
        // Create a panel with a grid layout to hold the current node information
        JPanel currentInfoPanel = new JPanel(new GridLayout(5, 2, 5, 5));
        // Add a titled border to the panel for visual separation
        currentInfoPanel.setBorder(BorderFactory.createTitledBorder("Current Node Info"));

        // Create labels and text fields to display current node information
        JLabel currentNodeIpLabel = new JLabel("Current Node IP:");
        currentNodeIpField = new JTextField("None");
        currentNodeIpField.setEditable(false);
        JLabel currentNodePortLabel = new JLabel("Current Node Port:");
        currentNodePortField = new JTextField("None");
        currentNodePortField.setEditable(false);
        JLabel currentNodeHashLabel = new JLabel("Current Node Hash ID:");
        currentNodeHashField = new JTextField("None");
        currentNodeHashField.setEditable(false);
        JLabel successorLabel = new JLabel("Successor:");
        successorField = new JTextField(String.valueOf("None"));
        successorField.setEditable(false);
        JLabel predecessorLabel = new JLabel("Predecessor:");
        predecessorField = new JTextField(String.valueOf("None"));
        predecessorField.setEditable(false);

        // Add labels and text fields to the current node information panel
        currentInfoPanel.add(currentNodeIpLabel);
        currentInfoPanel.add(currentNodeIpField);
        currentInfoPanel.add(currentNodePortLabel);
        currentInfoPanel.add(currentNodePortField);
        currentInfoPanel.add(currentNodeHashLabel);
        currentInfoPanel.add(currentNodeHashField);
        currentInfoPanel.add(successorLabel);
        currentInfoPanel.add(successorField);
        currentInfoPanel.add(predecessorLabel);
        currentInfoPanel.add(predecessorField);

        return currentInfoPanel;
    }

    /**
     * Creates a panel to handle the creation of a new Chord ring.
     * This panel includes input fields for specifying the IP address and port
     * number of the new node,
     * as well as a button to initiate the creation process.
     * Additionally, it includes a text field to display messages related to the
     * creation process.
     * 
     * @return JPanel representing the create panel.
     */
    private JPanel createCreatePanel() {
        // Create the main panel with a border layout
        JPanel createPanel = new JPanel(new BorderLayout());
        createPanel.setBorder(BorderFactory.createTitledBorder("Create")); // Add titled border

        // Create a panel to hold input fields with a grid layout
        JPanel fieldsPanel = new JPanel(new GridLayout(2, 2, 5, 5));

        // Create labels and text fields for node IP and port
        JLabel nodeIpLabel = new JLabel("Node IP:");
        JTextField nodeIpField = new JTextField(); // Make sure to declare locally
        JLabel nodePortLabel = new JLabel("Node Port:");
        JTextField nodePortField = new JTextField(); // Make sure to declare locally

        // Add labels and text fields to the fields panel
        fieldsPanel.add(nodeIpLabel);
        fieldsPanel.add(nodeIpField);
        fieldsPanel.add(nodePortLabel);
        fieldsPanel.add(nodePortField);

        // Add the fields panel to the center of the main panel
        createPanel.add(fieldsPanel, BorderLayout.CENTER);

        // Add a Create button
        JButton createButton = new JButton("Create");
        JTextField messageField = new JTextField("No Chord ring present");
        messageField.setEditable(false);

        // Add action listener to the Create button
        createButton.addActionListener(e -> {
            // Perform action when Create button is clicked
            String ip = nodeIpField.getText();
            int port = Integer.parseInt(nodePortField.getText());
            // Call method to create Chord ring and disable the button
            createChordRing(ip, port, createButton, messageField);
            createButton.setEnabled(false); // Disable the Create button
            messageField.setText("Chord ring created successfully.");
        });

        // Create a panel to center the button
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        buttonPanel.add(createButton);
        // Add the button panel to the right side of the main panel
        createPanel.add(buttonPanel, BorderLayout.EAST);

        // Add a text field to display messages at the bottom of the main panel
        createPanel.add(messageField, BorderLayout.SOUTH);

        return createPanel;
    }

    /**
     * Creates a new Chord ring with the specified IP address and port number.
     * This method initializes a new node, sets its predecessor and successor to
     * itself,
     * updates the finger table, and updates the GUI with the current node
     * information.
     * 
     * @param ip           The IP address of the new node.
     * @param port         The port number of the new node.
     * @param createButton The button used to create the Chord ring.
     * @param messageField The text field used to display messages related to the
     *                     Chord ring creation.
     */
    private void createChordRing(String ip, int port, JButton createButton, JTextField messageField) {
        System.out.println("CREATING RING: ");
        // Create a SwingWorker to perform background tasks
        SwingWorker<Void, Void> worker = new SwingWorker<>() {
            @Override
            protected Void doInBackground() {
                // Initialize a new node with the specified IP and port
                Node node = new Node(ip, port);
                // Set predecessor and successor for the first node
                node.predecessor = new Node(ip, port);
                node.successor = new Node(ip, port);
                // Update finger table
                node.fingerTable.table.get(0).setNode(node.successor);

                // Update current node information and GUI components
                curr_node = node;
                currNodeIP = ip;
                currNodePort = String.valueOf(port);
                currentNodeIpField.setText(ip);
                currentNodePortField.setText(String.valueOf(port));
                currentNodeHashField.setText(String.valueOf(curr_node.id));
                predecessorField.setText(String.valueOf(curr_node.id));
                successorField.setText(String.valueOf(curr_node.id));

                // Start the node
                node.start();
                return null; // Void return type
            }
        };
        // Execute the SwingWorker to perform background tasks
        worker.execute();
    }

    /**
     * Creates a panel to display data and information about a node in the Chord
     * ring.
     * This panel allows users to input the IP address and port of the node they
     * want to query,
     * and then displays the finger table, key-value data store, and other
     * information about that node.
     * 
     * @return The JPanel containing components to show data and information about a
     *         node.
     */
    private JPanel createShowPanel() {
        // Create the main panel with a border layout
        JPanel showPanel = new JPanel(new BorderLayout());
        showPanel.setBorder(BorderFactory.createTitledBorder("Show Data")); // Add titled border

        // Panel to hold input fields for node IP and port
        JPanel fieldsPanel = new JPanel(new GridLayout(2, 2, 5, 5));
        JLabel showNodeIpLabel = new JLabel("Node IP:");
        JTextField showNodeIpField = new JTextField(); // Make sure to declare locally
        JLabel showNodePortLabel = new JLabel("Node Port:");
        JTextField showNodePortField = new JTextField(); // Make sure to declare locally
        fieldsPanel.add(showNodeIpLabel);
        fieldsPanel.add(showNodeIpField);
        fieldsPanel.add(showNodePortLabel);
        fieldsPanel.add(showNodePortField);
        showPanel.add(fieldsPanel, BorderLayout.NORTH);

        // Output field to display "Key Value Data"
        JTextArea keyValueDataArea = new JTextArea(10, 40); // Rows, Columns
        keyValueDataArea.setEditable(false); // Make it read-only
        keyValueDataArea.setBorder(BorderFactory.createTitledBorder("Key Value Data")); // Add titled border
        JScrollPane keyValueScrollPane = new JScrollPane(keyValueDataArea); // Add scroll bar
        showPanel.add(keyValueScrollPane, BorderLayout.CENTER);

        // Output field to display "Finger Table"
        JTextArea fingerTableArea = new JTextArea(10, 40); // Rows, Columns
        fingerTableArea.setEditable(false); // Make it read-only
        fingerTableArea.setBorder(BorderFactory.createTitledBorder("Finger Table")); // Add titled border
        JScrollPane fingerTableScrollPane = new JScrollPane(fingerTableArea); // Add scroll bar
        showPanel.add(fingerTableScrollPane, BorderLayout.EAST);

        // Button to trigger showing the data
        JButton showButton = new JButton("Show");
        showButton.addActionListener(e -> {
            // Perform action when Show button is clicked
            String ip = showNodeIpField.getText();
            int port = Integer.parseInt(showNodePortField.getText());
            String message1 = "Get_Finger_Table", message2 = "Get_Data_Store", message3 = "Get_Info";

            // Fetch and display finger table data
            try (Socket socket = new Socket(ip, port);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    Scanner in = new Scanner(socket.getInputStream()).useDelimiter("\\A")) {
                out.println(message1);
                String fingerTableResponse = in.hasNext() ? in.next() : "";
                fingerTableArea.setText(fingerTableResponse);
            } catch (Exception ex) {
                fingerTableArea.setText("Error occurred: " + ex.getMessage());
            }

            // Fetch and display key-value data store
            try (Socket socket = new Socket(ip, port);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    Scanner in = new Scanner(socket.getInputStream()).useDelimiter("\\A")) {
                out.println(message2);
                String keyValueDataResponse = in.hasNext() ? in.next() : "";
                keyValueDataArea.setText(keyValueDataResponse);
            } catch (Exception ex) {
                keyValueDataArea.setText("Error occurred: " + ex.getMessage());
            }

            // Fetch and display node information
            try (Socket socket = new Socket(ip, port);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    Scanner in = new Scanner(socket.getInputStream()).useDelimiter("\\A")) {
                out.println(message3);
                String information = in.hasNext() ? in.next() : "";
                String[] info = information.split("/");
                currentNodeIpField.setText(info[0]);
                currentNodePortField.setText(info[1]);
                currentNodeHashField.setText(info[2]);
                predecessorField.setText(info[3]);
                successorField.setText(info[4]);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });

        // Panel to center the button
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        buttonPanel.add(showButton);
        showPanel.add(buttonPanel, BorderLayout.SOUTH); // Align the button to the bottom

        return showPanel;
    }

    /**
     * Creates a panel to insert data into the Chord DHT.
     * This panel contains input fields for key and value, along with an output
     * message area to display insertion results.
     * 
     * @return The JPanel containing components for data insertion.
     */
    private JPanel createInsertionPanel() {
        // Create the main panel with a border layout
        JPanel insertionPanel = new JPanel(new BorderLayout());
        insertionPanel.setBorder(BorderFactory.createTitledBorder("Insert Data")); // Add titled border

        // Panel to hold input fields for key and value
        JPanel fieldsPanel = new JPanel(new GridLayout(2, 2, 5, 5));
        JLabel keyLabel = new JLabel("Key:");
        JTextField keyField = new JTextField();
        JLabel valueLabel = new JLabel("Value:");
        JTextField valueField = new JTextField();
        fieldsPanel.add(keyLabel);
        fieldsPanel.add(keyField);
        fieldsPanel.add(valueLabel);
        fieldsPanel.add(valueField);
        insertionPanel.add(fieldsPanel, BorderLayout.NORTH);

        // Output message field
        JTextArea outputMessageArea = new JTextArea(5, 40); // Rows, Columns
        outputMessageArea.setEditable(false); // Make it read-only
        outputMessageArea.setBorder(BorderFactory.createTitledBorder("Output Message")); // Add titled border
        JScrollPane outputScrollPane = new JScrollPane(outputMessageArea); // Add scroll bar
        insertionPanel.add(outputScrollPane, BorderLayout.CENTER);

        // Add Insert button
        JButton insertButton = new JButton("Insert");
        insertButton.addActionListener(e -> {
            // Perform action when Insert button is clicked
            String key = keyField.getText();
            String value = valueField.getText();
            String message = "Insert|" + key + ":" + value;
            try (Socket socket = new Socket(currNodeIP, Integer.parseInt(currNodePort));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    Scanner in = new Scanner(socket.getInputStream())) {
                out.println(message);
                String response = in.nextLine();
                outputMessageArea.setText(response);
            } catch (Exception ex) {
                outputMessageArea.setText("Error occurred: " + ex.getMessage());
            }
        });

        // Panel to center the button
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        buttonPanel.add(insertButton);
        insertionPanel.add(buttonPanel, BorderLayout.SOUTH); // Align the button to the bottom

        return insertionPanel;
    }

    /**
     * Creates a panel to delete data from the Chord DHT.
     * This panel contains an input field for the key to be deleted and an output
     * message area to display deletion results.
     * 
     * @return The JPanel containing components for data deletion.
     */
    private JPanel createDeletionPanel() {
        // Create the main panel with a border layout
        JPanel deletionPanel = new JPanel(new BorderLayout());
        deletionPanel.setBorder(BorderFactory.createTitledBorder("Delete Data")); // Add titled border

        // Panel to hold input fields for the key
        JPanel fieldsPanel = new JPanel(new GridLayout(1, 2, 5, 5));
        JLabel keyLabel = new JLabel("Key:");
        JTextField keyField = new JTextField();
        fieldsPanel.add(keyLabel);
        fieldsPanel.add(keyField);
        deletionPanel.add(fieldsPanel, BorderLayout.NORTH);

        // Output field to display deletion message
        JTextArea outputMessageArea = new JTextArea(5, 40); // Rows, Columns
        outputMessageArea.setEditable(false); // Make it read-only
        outputMessageArea.setBorder(BorderFactory.createTitledBorder("Output Message")); // Add titled border
        JScrollPane outputScrollPane = new JScrollPane(outputMessageArea); // Add scroll bar
        deletionPanel.add(outputScrollPane, BorderLayout.CENTER);

        // Add Delete button
        JButton deleteButton = new JButton("Delete");
        deleteButton.addActionListener(e -> {
            // Perform action when Delete button is clicked
            String key = keyField.getText();
            String message = "Delete|" + key;
            try (Socket socket = new Socket(currNodeIP, Integer.parseInt(currNodePort));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    Scanner in = new Scanner(socket.getInputStream())) {
                out.println(message);
                String response = in.nextLine();
                outputMessageArea.setText(response);
            } catch (Exception ex) {
                outputMessageArea.setText("Error occurred: " + ex.getMessage());
            }
        });

        // Panel to center the button
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        buttonPanel.add(deleteButton);
        deletionPanel.add(buttonPanel, BorderLayout.SOUTH); // Align the button to the bottom

        return deletionPanel;
    }

    /**
     * Creates a panel to search for data in the Chord DHT.
     * This panel contains an input field for the key to be searched and an output
     * area to display the search result.
     * 
     * @return The JPanel containing components for data search.
     */
    private JPanel createSearchPanel() {
        // Create the main panel with a border layout
        JPanel searchPanel = new JPanel(new BorderLayout());
        searchPanel.setBorder(BorderFactory.createTitledBorder("Search Data")); // Add titled border

        // Panel to hold input fields for the key
        JPanel fieldsPanel = new JPanel(new GridLayout(1, 2, 5, 5));
        JLabel keyLabel = new JLabel("Key:");
        JTextField keyField = new JTextField();
        fieldsPanel.add(keyLabel);
        fieldsPanel.add(keyField);
        searchPanel.add(fieldsPanel, BorderLayout.NORTH);

        // Output field to display search result
        JTextArea outputResultArea = new JTextArea(5, 40); // Rows, Columns
        outputResultArea.setEditable(false); // Make it read-only
        outputResultArea.setBorder(BorderFactory.createTitledBorder("Search Result")); // Add titled border
        JScrollPane outputScrollPane = new JScrollPane(outputResultArea); // Add scroll bar
        searchPanel.add(outputScrollPane, BorderLayout.CENTER);

        // Add Search button
        JButton searchButton = new JButton("Search");
        searchButton.addActionListener(e -> {
            // Perform action when Search button is clicked
            String key = keyField.getText().trim(); // Get the key from the input field
            String message = "Search|" + key;
            try (Socket socket = new Socket(currNodeIP, Integer.parseInt(currNodePort));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    Scanner in = new Scanner(socket.getInputStream())) {
                out.println(message);
                String response = in.nextLine();
                outputResultArea.setText(response);
            } catch (Exception ex) {
                outputResultArea.setText("Error occurred: " + ex.getMessage());
            }
        });

        // Panel to center the button
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        buttonPanel.add(searchButton);
        searchPanel.add(buttonPanel, BorderLayout.SOUTH); // Align the button to the bottom

        return searchPanel;
    }

    /**
     * Creates a panel to join an existing Chord ring.
     * This panel contains input fields for the IP and port of the existing node and
     * the IP and port of the new node to be joined.
     * 
     * @return The JPanel containing components for joining an existing Chord ring.
     */
    private JPanel createJoinPanel() {
        // Create the main panel with a border layout
        JPanel joinPanel = new JPanel(new BorderLayout());
        joinPanel.setBorder(BorderFactory.createTitledBorder("Join")); // Add titled border

        // Panel for text fields
        JPanel fieldsPanel = new JPanel(new GridLayout(4, 2, 5, 5));

        JLabel existingNodeIpLabel = new JLabel("Existing Node IP:");
        JTextField existingNodeIpField = new JTextField(); // Make sure to declare locally
        JLabel existingNodePortLabel = new JLabel("Existing Node Port:");
        JTextField existingNodePortField = new JTextField(); // Make sure to declare locally
        JLabel newNodeIpLabel = new JLabel("New Node IP:");
        JTextField newNodeIpField = new JTextField(); // Make sure to declare locally
        JLabel newNodePortLabel = new JLabel("New Node Port:");
        JTextField newNodePortField = new JTextField(); // Make sure to declare locally

        fieldsPanel.add(existingNodeIpLabel);
        fieldsPanel.add(existingNodeIpField);
        fieldsPanel.add(existingNodePortLabel);
        fieldsPanel.add(existingNodePortField);
        fieldsPanel.add(newNodeIpLabel);
        fieldsPanel.add(newNodeIpField);
        fieldsPanel.add(newNodePortLabel);
        fieldsPanel.add(newNodePortField);

        joinPanel.add(fieldsPanel, BorderLayout.CENTER);

        // Add Join button
        JButton joinButton = new JButton("Join");
        joinButton.addActionListener(e -> {
            // Perform action when Join button is clicked
            String existingIp = existingNodeIpField.getText();
            int existingPort = Integer.parseInt(existingNodePortField.getText());
            String newIp = newNodeIpField.getText();
            int newPort = Integer.parseInt(newNodePortField.getText());
            joinChordRing(existingIp, existingPort, newIp, newPort);
        });

        // Create a panel to center the button
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        buttonPanel.add(joinButton);

        joinPanel.add(buttonPanel, BorderLayout.SOUTH); // Align the button to the bottom

        return joinPanel;
    }

    /**
     * Method to join an existing chord ring in a separate thread.
     * 
     * @param existingIp   The IP address of the existing node.
     * @param existingPort The port number of the existing node.
     * @param newIp        The IP address of the new node to be joined.
     * @param newPort      The port number of the new node to be joined.
     */
    private void joinChordRing(String existingIp, int existingPort, String newIp, int newPort) {
        System.out.println("Joining Ring");
        SwingWorker<Void, Void> worker = new SwingWorker<>() {
            @Override
            protected Void doInBackground() {
                Node node = new Node(newIp, newPort);
                node.join(existingIp, existingPort);
                node.start();
                curr_node = node;
                currNodeIP = newIp;
                currNodePort = String.valueOf(newPort);
                currentNodeIpField.setText(newIp);
                currentNodePortField.setText(String.valueOf(newPort));
                currentNodeHashField.setText(String.valueOf(curr_node.id));
                predecessorField.setText(String.valueOf(curr_node.predecessor.id));
                successorField.setText(String.valueOf(curr_node.successor.id));
                return null;
            }
        };
        worker.execute();
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new Chord_DHT_GUI());
    }
}
