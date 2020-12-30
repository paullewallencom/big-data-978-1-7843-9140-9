package chapter.nine;

import java.net.*;
import java.io.*;

/**
 * Reads the given input from the console and sends it to a socket for
 * consumption by external applications .
 *
 */
public class StreamProducer {

	public static void main(String[] args) {

		if (args == null || args.length < 1) {
			System.out.println("Usage - java chapter.eleven.StreamProducer <port#>");
			System.exit(0);
		}
		System.out.println("Defining new Socket on " + args[0]);
		try (ServerSocket soc = new ServerSocket(Integer.parseInt(args[0]))) {

			System.out.println("Waiting for Incoming Connection on - "
					+ args[0]);
			Socket clientSocket = soc.accept();

			System.out.println("Connection Received");
			OutputStream outputStream = clientSocket.getOutputStream();
			// Keep Reading the data in a Infinite loop and send it over to the
			// Socket.
			while (true) {
				PrintWriter out = new PrintWriter(outputStream, true);
				BufferedReader read = new BufferedReader(new InputStreamReader(
						System.in));
				System.out.println("Waiting for user to input some data");
				String data = read.readLine();
				System.out
						.println("Data received and now writing it to Socket");
				out.println(data);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
