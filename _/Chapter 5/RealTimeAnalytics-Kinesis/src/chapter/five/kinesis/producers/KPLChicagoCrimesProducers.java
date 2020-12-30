package chapter.five.kinesis.producers;

import java.io.*;
import java.nio.*;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.producer.*;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.*;

public class KPLChicagoCrimesProducers {

	KinesisProducer producer;
	
	FutureCallback<UserRecordResult> callback;
	
	//Location of the file from where we need to read the data
	private String filePath = "C:\\myWork\\MY-Books\\PackPub\\RealTimeAnalytics-WithShilpi\\Chapters\\SampleDataSet\\Crimes_-Aug-2015.csv";

	/**
	 * Define all initial/ important configurations, 
	 * required for Initializing the Producer
	 */
	public KPLChicagoCrimesProducers() {
		KinesisProducerConfiguration config = new KinesisProducerConfiguration();
		//Max Time (milliseconds) for which a record should be kept in a buffer before submitting
		config.setRecordMaxBufferedTime(1000);
		//Max number of connections for sending HTTP request  
		config.setMaxConnections(1);
		//Timeout in milliseconds for the HTTP request 
		config.setRequestTimeout(4000);
		//Name of the AWS region
		config.setRegion("us-east-1");
		//Provide AWS Credentials
		AWSCredentialsProvider credentials = new ProfileCredentialsProvider("kinesisCred");
		System.out.println("Access Key = " + credentials.getCredentials().getAWSAccessKeyId());
		System.out.println("Secret Key = " + credentials.getCredentials().getAWSSecretKey());
		config.setCredentialsProvider(credentials);
		producer = new KinesisProducer(config);

		//Implementation of Future Call Back, so that we receive all
		//Success and failure messages and then can take appropriate 
		//Actions.
		callback = new FutureCallback<UserRecordResult>() {
			@Override
			public void onFailure(Throwable t) {
				
				if (t instanceof UserRecordFailedException) {
                    Attempt first = Iterables.getFirst(((UserRecordFailedException) t).getResult().getAttempts(), null);
                    //Print the error and then exit.
                    System.out.println("Got an Error whiile submmitting records");
                    System.out.println("Error Code = "+first.getErrorCode()+", Error Message = "+first.getErrorMessage());        
                }

				//Stop all processing Immediately as we are receiving errors while submitting
				producer.destroy();
			};

			@Override
			public void onSuccess(UserRecordResult result) {
				//Successfully Submitted the records
				System.out.println("Succesfully Submitted the record, ShardID = "+result.getShardId()+", sequenceNumber = "+result.getSequenceNumber());
			};
		};

	}

	/**
	 * Plain/ Vanilla implementation of KPL where we read the records and 
	 *just submit to the Kinesis Streams
	 * @param streamName - name of the Stream
	 */
	public void basicCreateAndSubmit(String streamName) {

		String data = "";
		try (BufferedReader br = new BufferedReader(new FileReader(new File(
				filePath)))) {
			//Skipping first line as it has headers;
			br.readLine();
			//Read Complete file - Line by Line
			while ((data = br.readLine()) != null) {
				ByteBuffer dataBuffer = ByteBuffer.wrap(data.getBytes("UTF-8"));
				// Add records to the Producer. It is non-blocking as records are buffered.
				producer.addUserRecord(streamName, (data.split(","))[4],
						dataBuffer);
				System.out.println("Record Submitted");
				//Take a breath for 1/10 of a second before 
				//reading and submitting next message
				Thread.sleep(100);

			}
		} catch (Exception e) {
			// Print all exceptions
			e.printStackTrace();
		}
	}
	
	/**
	 * Advance implementation of KPL where we submit the messages
	 * and check their outcome in an Asynchronous manner using 
	 * callback mechanism.   
	 * @param streamName - Name of Stream
	 */
	public void aSynchronousCreateAndSubmit(String streamName) {

		String data = "";
		try (BufferedReader br = new BufferedReader(new FileReader(new File(
				filePath)))) {
			//Skipping first line as it has headers;
			br.readLine();
			//Read Complete file - Line by Line
			while ((data = br.readLine()) != null) {
				ByteBuffer dataBuffer = ByteBuffer.wrap(data.getBytes("UTF-8"));
				// capture the result of submitting the records to producer
				ListenableFuture<UserRecordResult> result = producer.addUserRecord(streamName,
						(data.split(","))[4], dataBuffer);
				System.out.println("Records Added to the Producer");
				//Register the result to the custom implementation of FutureCallback
				//which will be invoked when records are actually submitted to
				//Kinesis Streams
				Futures.addCallback(result, callback);
				//Wait for 1/10 of a second before submitting next record
				Thread.sleep(100);
			}
			
		} catch (Exception e) {
			// Print all exceptions
			e.printStackTrace();
		}

	}


}
