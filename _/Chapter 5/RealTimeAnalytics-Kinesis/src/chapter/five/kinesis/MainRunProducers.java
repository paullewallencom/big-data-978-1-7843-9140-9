package chapter.five.kinesis;

import chapter.five.kinesis.admin.ManageKinesisStreams;
import chapter.five.kinesis.producers.*;

public class MainRunProducers {

	//This Stream will be used by the producers/ consumers using KPL or KCL API's
	public static String streamName = "StreamingService";
	//This Stream will be used by the producers/ consumers using AWS SDK
	public static String streamNameAWS = "AWSStreamingService";
	
	public static void main(String[] args) {
		ManageKinesisStreams streams = new ManageKinesisStreams();
		streams.createKinesisStream(streamName, 1);
		streams.createKinesisStream(streamNameAWS, 1);
		
		//Using AWS Native API's
		AWSChicagoCrimesProducers producers = new AWSChicagoCrimesProducers();
		//Read and Submit record by record
		//producers.readSingleRecordAndSubmit(streamName);
		//Submit the records in Batches
		producers.readAndSubmitBatch(streamNameAWS, 10);
		
		//Using KPL
		KPLChicagoCrimesProducers kplProducers= new KPLChicagoCrimesProducers();
		//kplProducers.basicCreateAndSubmit(streamName);
		kplProducers.aSynchronousCreateAndSubmit(streamName);
		
		//Enable only when you want to Delete the streams
		//streams.deleteKinesisStream(streamNameAWS);
		//streams.deleteKinesisStream(streamName);

	}

}
