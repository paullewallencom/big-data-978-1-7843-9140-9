package chapter.nine;

import java.io.*;
import java.net.*;
import java.util.Random;

public class CrimeProducer {

	public static void main(String[] args) {

		if (args == null || args.length < 1) {
			System.out
					.println("Usage - java chapter.eleven.CrimeProducer <port#>");
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
				// Getting new Random Integer between 0 and 60
				int recordsToRead = number.nextInt(60);

				System.out.println("Records to Read = " + recordsToRead);
				for (int i = 0; i < recordsToRead; i++) {
					String dataLine = brReader.readLine() + "\n";
					dataBuilder.append(dataLine);
				}
				System.out
						.println("Data received and now writing it to Socket");
				out.println(dataBuilder);
				out.flush();
				// Sleep for 6 Seconds before reading again
				Thread.sleep(6000);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
