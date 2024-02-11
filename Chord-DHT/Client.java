import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

/**
 * This class represents a client that connects to a server to perform data operations.
 */
public class Client {

    /**
     * Main method to run the client application.
     * 
     * @param args Command-line arguments (not used in this program).
     */
    public static void main(String[] args) {
        String ip = "192.168.240.106"; // IP address of the server
        Scanner scanner = new Scanner(System.in);

        System.out.print("Give the port number of a node: ");
        int port = scanner.nextInt(); // Port number of the server

        try {
            while (true) {
                // Displaying menu options
                System.out.println("+++++++++++++++++++++++++MENU ++++++++++++++++++++++++");
                System.out.println("PRESS ++++++++++++++++++++++++++++++++++++++++++++++++");
                System.out.println("1. TO ENTER DATA +++++++++++++++++++++++++++++++++++++");
                System.out.println("2. TO SHOW DATA  +++++++++++++++++++++++++++++++++++++");
                System.out.println("3. TO DELETE DATA ++++++++++++++++++++++++++++++++++++");
                System.out.println("4. TO EXIT +++++++++++++++++++++++++++++++++++++++++++");
                System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                System.out.print("Choice: ");
                String choice = scanner.next();

                try (Socket socket = new Socket(ip, port);
                     PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                     Scanner in = new Scanner(socket.getInputStream())) {

                    if (choice.equals("1")) {
                        // Option to insert data
                        System.out.print("ENTER THE KEY : ");
                        String key = scanner.next();
                        System.out.print("ENTER THE VALUE : ");
                        String val = scanner.next();
                        String message = "Insert|" + key + ":" + val;
                        out.println(message);
                        String response = in.nextLine();
                        System.out.println(response);
                    } else if (choice.equals("2")) {
                        // Option to search for data
                        System.out.print("ENTER THE KEY : ");
                        String key = scanner.next();
                        String message = "Search|" + key;
                        out.println(message);
                        String response = in.nextLine();
                        System.out.println("The value corresponding to the key is : " + response);
                    } else if (choice.equals("3")) {
                        // Option to delete data
                        System.out.print("ENTER THE KEY : ");
                        String key = scanner.next();
                        String message = "Delete|" + key;
                        out.println(message);
                        String response = in.nextLine();
                        System.out.println(response);
                    } else if (choice.equals("4")) {
                        // Option to exit the program
                        System.out.println("Closing the socket");
                        System.out.println("Exiting Client");
                        break;
                    } else {
                        System.out.println("INCORRECT CHOICE");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } finally {
            scanner.close();
        }
    }
}
