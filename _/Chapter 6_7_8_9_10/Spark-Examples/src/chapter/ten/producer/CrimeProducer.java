package chapter.ten.producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class CrimeProducer {

	public static void main(String[] args) {

		if (args == null || args.length < 1) {
			System.out
					.println("Usage - java chapter.twelve.CrimeProducer <port#>");
			System.exit(0);
		}
		System.out.println("Defining new Socket on " + args[0]);
		try (ServerSocket soc = new ServerSocket(Integer.parseInt(args[0]))) {

			System.out.println("Waiting for Incoming Connection on - "
					+ args[0]);
			Socket clientSocket = soc.accept();

			System.out.println("Connection Received");
			OutputStream outputStream = clientSocket.getOutputStream();
			// Path of the file from where we need to read crime records.
			String filePath = "/home/ec2-user/softwares/crime-data/Crimes_-Aug-2015.csv";
			PrintWriter out = new PrintWriter(outputStream, true);
			BufferedReader brReader = new BufferedReader(new FileReader(
					filePath));
			// Defining Random number to read different number of records each
			// time.
			Random number = new Random();
			// Keep Reading the data in a Infinite loop and send it over to the
			// Socket.
			while (true) {
				System.out.println("Reading Crime Records");
				StringBuilder dataBuilder = new StringBuilder();
				// Getting new Random Integer between 0 and 10
				int recordsToRead = number.nextInt(20);

				System.out.println("Records to Read = " + recordsToRead);
				for (int i = 0; i < recordsToRead; i++) {
					String dataLine = brReader.readLine() + "\n";
					dataBuilder.append(dataLine);
				}
				System.out
						.println("Data received and now writing it to Socket");
				out.println(dataBuilder);
				out.flush();
				// Sleep for 20 Seconds before reading again
				Thread.sleep(20000);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
