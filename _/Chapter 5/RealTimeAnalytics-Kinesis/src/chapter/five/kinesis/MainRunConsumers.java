package chapter.five.kinesis;

import chapter.five.kinesis.admin.ManageKinesisStreams;
import chapter.five.kinesis.consumers.AWSChicagoCrimesConsumers;
import chapter.five.kinesis.consumers.KCLChicagoCrimesConsumers;

public class MainRunConsumers {

	//This Stream will be used by the producers/ consumers using KPL or KCL API's
	public static String streamName = "StreamingService";
	//This Stream will be used by the producers/ consumers using AWS SDK
	public static String streamNameAWS = "AWSStreamingService";
	
	public static void main(String[] args) {

		//Using AWS Native API's
		AWSChicagoCrimesConsumers consumers = new AWSChicagoCrimesConsumers();
		consumers.consumeAndAlert(streamNameAWS);
		
		//Using KCL
		//KCLChicagoCrimesConsumers kclConsumers= new KCLChicagoCrimesConsumers(streamName);
		//kclConsumers.consumeAndAlert();
		
		//Enable only when you want to Delete the streams
		ManageKinesisStreams streams = new ManageKinesisStreams();
		//streams.deleteKinesisStream(streamNameAWS);
		//streams.deleteKinesisStream(streamName);

	}

}
