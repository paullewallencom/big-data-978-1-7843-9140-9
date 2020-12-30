package chapter.five.kinesis.producers;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;

public class AWSChicagoCrimesProducers{

	private AmazonKinesisClient kClient;
	
	//Location of the file from where we need to read the data
	private String filePath="C:\\myWork\\MY-Books\\PackPub\\RealTimeAnalytics-WithShilpi\\Chapters\\SampleDataSet\\Crimes_-Aug-2015.csv";

	/**
	 * Constructor which initialize the Kinesis Client for working with the
	 * Kinesis streams.
	 */
	public AWSChicagoCrimesProducers() {
		// Initialize the AWSCredentials taken from the profile configured in
		// the Eclipse
		// replace "kinesisCred" with the profile configured in your Eclipse or
		// leave blank to sue default.
		// Ensure that Access and Secret key are present in in credentials file
		// Default location of credentials file is $USER_HOME/.aws/credentials
		AWSCredentials cred = new ProfileCredentialsProvider("kinesisCred")
				.getCredentials();
		System.out.println("Access Key = " + cred.getAWSAccessKeyId());
		System.out.println("Secret Key = " + cred.getAWSSecretKey());
		kClient = new AmazonKinesisClient(cred);
	}

	/**
	 * Read Each record of the input file and Submit each record to Amazon Kinesis Streams.  
	 * @param streamName - Name of the Stream.
	 */
	public void readSingleRecordAndSubmit(String streamName) {

		String data = "";
		
		try (BufferedReader br = new BufferedReader(
				new FileReader(new File(filePath)))) {
			//Skipping first line as it has headers;
			br.readLine();
			//Read Complete file - Line by Line
			while ((data = br.readLine()) != null) {
				//Create Record Request
				PutRecordRequest putRecordRequest = new PutRecordRequest();
				putRecordRequest.setStreamName(streamName);
				putRecordRequest.setData(ByteBuffer.wrap((data.getBytes())));
				//Data would be partitioned by the IUCR COdes, which is 5 column in the record
				String IUCRcode = (data.split(","))[4]; 
				putRecordRequest.setPartitionKey(IUCRcode);
				//Finally Submit the records with Kinesis CLient Object
				System.out.println("Submitting Record = "+data);
				kClient.putRecord(putRecordRequest);
				//Sleep for half a second before we read and submit next record.
				Thread.sleep(500);
			}
		} catch (Exception e) {
			//Print exceptions, in case any
			e.printStackTrace();
		}

	}
	
	/**
	 * Read Data line by line by Submit to Kinesis Streams in the Batches. 
	 * @param streamName - Name of Stream
	 * @param batchSize - Batch Size
	 */
	public void readAndSubmitBatch(String streamName, int batchSize) {

		String data = "";
		try (BufferedReader br = new BufferedReader(
				new FileReader(new File(filePath)))) {

			//Skipping first line as it has headers;
			br.readLine();
			//Counter to keep track of Batch
			int counter = 0;
			//COllection which will contain the batch of records
			ArrayList<PutRecordsRequestEntry> recordRequestEntryList = new ArrayList<PutRecordsRequestEntry>();
			while ((data = br.readLine()) != null) {
				//Read Data and Crreate Object of PutRecordsRequestEntry
				PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
				entry.setData(ByteBuffer.wrap((data.getBytes())));
				//Data would be partitioned by the IUCR COdes, which is 5 column in the record
				String IUCRcode = (data.split(","))[4];
				entry.setPartitionKey(IUCRcode);
				//Add the record the Collection
				recordRequestEntryList.add(entry);
				//Increment the Counter
				counter++;

				//Submit Records in case Btach size is reached.
				if (counter == batchSize) {
					PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
					putRecordsRequest.setRecords(recordRequestEntryList);
					putRecordsRequest.setStreamName(streamName);
					//Finally Submit the records with Kinesis CLient Object
					System.out.println("Submitting Records = "+recordRequestEntryList.size());
					kClient.putRecords(putRecordsRequest);
					//Reset the collection and Counter/ Batch
					recordRequestEntryList = new ArrayList<PutRecordsRequestEntry>();
					counter = 0;
					//Sleep for half a second before processing another batch
					Thread.sleep(500);
				}
			}
		} catch (Exception e) {
			//Print all exceptions
			e.printStackTrace();
		}

	}


}
