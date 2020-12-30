package chapter.five.kinesis.consumers;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.*;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.*;

/**
 * Consumer using the KCL library for consuming the records
 * @author sagupta
 *
 */
public class KCLChicagoCrimesConsumers {
	
	private KinesisClientLibConfiguration kinesisClientLibConfiguration;
	
	/**
	 * Constructor which initialize the Kinesis Client for working with the
	 * Kinesis streams.
	 */
	public KCLChicagoCrimesConsumers(String streamName) {
		// Create Object of KinesisClientLibConfiguration which takes 4 parameters: -
		//1. User Defined "ApplicaitonName", used for creating tables in DynamoDB -
		//2. Name of the Kinesis Stream from we will read the data
		//3. AWS Credentials
		//4. Unique ID of the Worker. We can create multiple Objects 
		//  of KinesisConsumerLibConfiguration which can consume data from same streams.
		kinesisClientLibConfiguration = new KinesisClientLibConfiguration("myStreamingApp",streamName,new ProfileCredentialsProvider("kinesisCred"),"worker-1");
		//Position in the stream from where we need to read the records
		//LATEST = Start after the most recent data record (fetch new data).
		//TRIM_HORIZON = Start from the oldest available data record.
        kinesisClientLibConfiguration.withInitialPositionInStream(InitialPositionInStream.LATEST);
	}
	
	public void consumeAndAlert(){		
        //Define the Factory for processing the records
        IRecordProcessorFactory recordProcessorFactory = new IRecordProcessorFactory(){
        	public IRecordProcessor createProcessor(){
        		return new KCLChicagoCrimeRecordProcessor();
        	}
        };
        //Initialize the worker with record processor factory and client Config
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

        int exitCode = 0;
        try {
        	//Start the worker
            worker.run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);

		
	}


}
