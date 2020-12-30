package chapter.five.kinesis.consumers;

import java.nio.charset.*;
import java.util.List;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.*;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.*;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
/**
 * Implementation of Record Processor using KCL library which processes each received records 
 * @author sagupta
 *
 */

public class KCLChicagoCrimeRecordProcessor implements IRecordProcessor{

  
	String kinesisShardId;
	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
	
	/**
	 * Act like a Constructor and Initialize the worker to receive records from a Shard
	 */
    public void initialize(String shardId){
    	System.out.println("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
    }

    /**
     * Actual method invoked for each record
     */
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer){
    	try{
    	for(Record record: records){
    		//Decode and convert Data received from ByteBuffer to string
    		String data = decoder.decode(record.getData()).toString();
    		System.out.println("Data received from Shard - "+kinesisShardId +" = "+data);
    		//Check if the IUCR of Crime is either 1310 0r 1320 or, then raise Alert
    		String iucrCode = record.getPartitionKey();
    		if("1320".equalsIgnoreCase(iucrCode) || "1310".equalsIgnoreCase(iucrCode)){
    			System.out.println("ALERT!!!!!!!! IMMEDIATE ACTION REQUIRED !!!!!!");
    		}
    		//Put a checkpoint to read from we left. 
    		//State will be saved in Dynamo DB, so it can survive crashes/ shutdowns 
    		//across the workers 
    		checkpointer.checkpoint();
    		
    		
    	}
    	}catch(Exception e ){
    		//Print all exceptions
    		e.printStackTrace();
    	}
    	
    	
    }

    
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason){
    	System.out.println("Shutting Down because = "+reason);
    	try{
    	//Put a checkpoint to read from we left
    	checkpointer.checkpoint();
    	}catch(ShutdownException | InvalidStateException e)
    	{
    		System.out.println("Caught exception while shutting Down");
    	}
    	
    }

}
