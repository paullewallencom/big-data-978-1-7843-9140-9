package chapter.five.kinesis.consumers;

import java.nio.charset.*;
import java.util.*;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;

/**
 * Kinesis Consumer retrieving Data using AWS Kinesis API's
 * 
 * @author sagupta
 *
 */
public class AWSChicagoCrimesConsumers {

	private AmazonKinesisClient kClient;
	private AWSCredentials credentials;
	private final CharsetDecoder decoder = Charset.forName("UTF-8")
			.newDecoder();

	/**
	 * Constructor which initialize the Kinesis Client for working with the
	 * Kinesis streams.
	 */
	public AWSChicagoCrimesConsumers() {
		// Initialize the AWSCredentials taken from the profile configured in
		// the Eclipse
		// replace "kinesisCred" with the profile configured in your Eclipse or
		// leave blank to sue default.
		// Ensure that Access and Secret key are present in in credentials file
		// Default location of credentials file is $USER_HOME/.aws/credentials
		credentials = new ProfileCredentialsProvider("kinesisCred")
				.getCredentials();
		kClient = new AmazonKinesisClient(credentials);
	}

	/**
	 * Retrieves the information about the Shards from the given STream
	 * 
	 * @param streamName
	 *            - Name of Stream
	 * @return - List of available Shards
	 */
	private List<Shard> getShards(String streamName) {

		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(streamName);
		List<Shard> shards = new ArrayList<>();
		String exclusiveStartShardId = null;
		do {
			describeStreamRequest
					.setExclusiveStartShardId(exclusiveStartShardId);
			DescribeStreamResult describeStreamResult = kClient
					.describeStream(describeStreamRequest);
			shards.addAll(describeStreamResult.getStreamDescription()
					.getShards());
			if (describeStreamResult.getStreamDescription().getHasMoreShards()
					&& shards.size() > 0) {
				exclusiveStartShardId = shards.get(shards.size() - 1)
						.getShardId();
			} else {
				exclusiveStartShardId = null;
			}
		} while (exclusiveStartShardId != null);

		return shards;
	}

	/**
	 * Fetches the data from the Given Stream after every 1 Second.
	 * 
	 * @param streamName
	 *            - Name of Stream
	 */
	public void consumeAndAlert(String streamName) {

		// Get all the List of Shards
		List<Shard> shards = getShards(streamName);
		System.out.println("Got the Shards. Total Shard = " + shards.size());
		// Check the size of Shards. It should be greater then 0
		// And take the first one and start iterating over it
		// for subsequent shards in the Iterator create different Threads for
		// processing
		// records received from each shard
		if (shards.size() > 0) {
			System.out.println("Getting Data from Shard- "
					+ shards.get(0).getShardId());
			processShard(shards.get(0), streamName);
		}

	}

	/**
	 * Fetches the data from each shard and further generate ALerts
	 * 
	 * @param shard
	 *            - Shard Object
	 * @param streamName
	 *            - Name of Stream
	 */
	private void processShard(Shard shard, String streamName) {
		// Prepare GetShardIteratorRequest object for initiating the request
		GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
		getShardIteratorRequest.setStreamName(streamName);
		// Define the Type of ShardIterator.
		// For more info refer to -
		// http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html
		// #Kinesis-GetShardIterator-request-ShardIteratorType
		getShardIteratorRequest.setShardIteratorType(ShardIteratorType.LATEST);
		// Set the Shard ID
		getShardIteratorRequest.setShardId(shard.getShardId());

		// Get the ShardIterator
		GetShardIteratorResult shardIteratorResult = kClient
				.getShardIterator(getShardIteratorRequest);

		String shardIterator = shardIteratorResult.getShardIterator();
		// Now infinitely read the records after every 1 second
		for (;;) {
			// Prepare GetRecordsRequest, Get the results and further process
			// it.
			GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
			getRecordsRequest.setRequestCredentials(credentials);
			getRecordsRequest.setShardIterator(shardIterator);
			// Limiting the records to 20 in each request. default is 10 Mb
			getRecordsRequest.setLimit(20);

			GetRecordsResult result = kClient.getRecords(getRecordsRequest);
			System.out.println("Number of records received = "
					+ result.getRecords().size());
			for (Record record : result.getRecords()) {
				System.out.println("Partition Key = "
						+ record.getPartitionKey());
				System.out.println("Sequence Number = "
						+ record.getSequenceNumber());
				try {
					// Decode and convert Data received from ByteBuffer to
					// string
					String data = decoder.decode(record.getData()).toString();
					System.out.println("Data = " + data);
					String iucrCode = record.getPartitionKey();
					// Generate Alert in case the Crime is Serious
					if ("1320".equalsIgnoreCase(iucrCode)
							|| "1310".equalsIgnoreCase(iucrCode)) {
						System.out
								.println("ALERT!!!!!!!! IMMEDIATE ACTION REQUIRED !!!!!!");
					}
				} catch (CharacterCodingException except) {
					except.printStackTrace();
				}

				// Will continue to check and retrieve data every 1 Second
				try {
					Thread.sleep(1000);
				} catch (InterruptedException interrupt) {
				}
				shardIterator = result.getNextShardIterator();

			}

		}

	}
}