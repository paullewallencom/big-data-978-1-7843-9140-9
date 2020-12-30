package chapter.five.kinesis.admin;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;

public class ManageKinesisStreams {

	private AmazonKinesisClient kClient;

	/**
	 * Constructor which initialize the Kinesis Client for working with the 
	 * Kinesis streams.
	 */
	public ManageKinesisStreams() {

		//Initialize the AWSCredentials taken from the profile configured in the Eclipse
		//replace "kinesisCred" with the profile configured in your Eclipse or leave blank to sue default.
		//Ensure that Access and Secret key are present in in credentials file
		//Default location of credentials file is $USER_HOME/.aws/credentials
		AWSCredentials cred = new ProfileCredentialsProvider("kinesisCred")
				.getCredentials();
		System.out.println("Access Key = "+ cred.getAWSAccessKeyId());
		System.out.println("Secret Key = " + cred.getAWSSecretKey());
		kClient = new AmazonKinesisClient(cred);

	}

	/**
	 * Create a Kinesis Stream with the given name and the shards.
	 * @param streamName - Name of the Stream
	 * @param shards - Number of Shards for the given Stream
	 */
	public void createKinesisStream(String streamName, int shards) {

		System.out.println("Creating new Stream = '"+streamName+"', with Shards = "+shards);
		//Check and create stream only if does not exist.
		if (!checkStreamExists(streamName)) {
			//CreateStreamRequest for creating Kinesis Stream
			CreateStreamRequest createStreamRequest = new CreateStreamRequest();
			createStreamRequest.setStreamName(streamName);
			createStreamRequest.setShardCount(shards);
			kClient.createStream(createStreamRequest);

			try {
				//Sleep for 30 seconds so that stream is initialized and created
				Thread.sleep(30000);

			} catch (InterruptedException e) {
				//No need to Print Anything
			}
		}
	}

	/**
	 * Checks and delete a given Kinesis Stream
	 * 
	 * @param streamName
	 *            - Name of the Stream
	 */

	public void deleteKinesisStream(String streamName) {

		//Check and delete stream only if exists.
		if (checkStreamExists(streamName)) {
			kClient.deleteStream(streamName);
			System.out.println("Deleted the Kinesis Stream = '" + streamName+"'");
			return;
		}

		System.out.println("Stream does not exists = " + streamName);

	}

	/**
	 * Utility Method which checks whether a given Kinesis Stream Exists or Not
	 * 
	 * @param streamName
	 *            - Name of the Stream
	 * @return - True in case Stream already exists else False
	 */
	public boolean checkStreamExists(String streamName) {

		try {
			//DescribeStreamRequest for Describing and checking the 
			//existence of given Kinesis Streams.
			DescribeStreamRequest desc = new DescribeStreamRequest();
			desc.setStreamName(streamName);
			DescribeStreamResult result = kClient.describeStream(desc);

			System.out.println("Kinesis Stream '" +streamName+ "' already exists...");
			System.out.println("Status of '"+ streamName + "' = "
					+ result.getStreamDescription().getStreamStatus());

		} catch (ResourceNotFoundException exception) {
			System.out.println("Stream '"+streamName+"' does Not exists...Need to create One");
			return false;
		}

		return true;
	}
	
	/**
	 * Main Method. Used just for testing
	 * @param args
	 */
	public static void main(String[] args) {
		ManageKinesisStreams stream = new ManageKinesisStreams();
		stream.createKinesisStream("StreamingService", 1);
		System.out.println("Checking whether Stream Exist or not");
		stream.checkStreamExists("StreamingService");
		stream.deleteKinesisStream("StreamingService");
	}

}
