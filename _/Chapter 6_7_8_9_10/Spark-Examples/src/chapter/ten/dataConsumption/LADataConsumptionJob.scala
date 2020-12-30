package chapter.ten.dataConsumption

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd._
import org.apache.spark.streaming.dstream._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._

object LADataConsumptionJob {

  def main(args: Array[String]) {
    println("Creating Spark Configuration")
    //Create an Object of Spark Configuration
    val conf = new SparkConf()
    //Set the logical and user defined Name of this Application
    conf.setAppName("LA Data Consumption Job")
    //Set the Cassandra Configurations 
    conf.set("spark.cassandra.connection.host", "localhost")

    println("Retreiving Streaming Context from Spark Conf")
    //Retrieving Streaming Context from SparkConf Object.
    //Second parameter is the time interval at which streaming data will be divided into batches  
    val streamCtx = new StreamingContext(conf, Seconds(60))

    //Define the the type of Stream. Here we are using TCP Socket as text stream, 
    //It will keep watching for the incoming data from a specific machine (localhost) and port (provided as argument) 
    //Once the data is retrieved it will be saved in the memory and in case memory
    //is not sufficient, then it will store it on the Disk
    //It will further read the Data and convert it into DStream
    val lines = streamCtx.socketTextStream("localhost", args(0).toInt, MEMORY_AND_DISK_SER_2)
    //AS a first Step persist the Raw data into a structured format
    //int the Append Only Master Table.
    persistInMaster(streamCtx,lines)
    //In Next step now generate the real-time 
    //view of the data received from oour Crime Producer 
    generateRealTimeView(streamCtx,lines)
    //Most important statement which will initiate the Streaming Context
    streamCtx.start();
    //Wait till the execution is completed.
    streamCtx.awaitTermination();  
    
  }
  
  /**
   * Function for persisting the raw records into a pre-defined format 
   * in master Table. 
   */
  def persistInMaster(streamCtx:StreamingContext,lines:DStream[String]){
    
    //Define Keyspace
    val keyspaceName ="lambdaarchitecture"
    //Define Table for persisting Master records
    val csMasterTableName="masterdata"
    
    lines.foreachRDD { 
      x => 
      //Splitting, flattening and finally filtering to exclude any Empty Rows
      val rawCrimeRDD = x.map(_.split("\n")).flatMap { x => x }.filter { x => x.length()>2 }
      println("Master Data Received = "+rawCrimeRDD.collect().length)
      
      //Splitting again for each Distinct value in the Row and creating Scala SEQ
      val splitCrimeRDD = rawCrimeRDD.map { x => x.split(",") }.map(c => createSeq(c))
      println("Now creating Sequence Persisting")
      //Finally Flattening the results and presisting in the table.
      val crimeRDD = splitCrimeRDD.flatMap(f=>f)
      crimeRDD.saveToCassandra(keyspaceName, csMasterTableName, SomeColumns("id","casenumber","date","block","iucr", "primarytype", "description"))
    }
  }
  

  //Utility method for creating Scala SEQ 
  def createSeq(m: Array[String]): Seq[(String, String, String, String, String, String,String)] = {
  
    //TO be Printed in Executor Logs
    println("value of m(0) = "+m(0))
    println("value of m(1) = "+m(1))
    println("value of m(2) = "+m(2))
    println("value of m(3) = "+m(3))
    println("value of m(4) = "+m(4))
    println("value of m(5) = "+m(5))
    println("value of m(6) = "+m(6))
    
    Seq((m(0),m(1),m(2),m(3),m(4),m(5),m(6)))
  }
  
  /**
   * This function process the stream of records 
   * and persist in the realtime views 
   */
  def generateRealTimeView(streamCtx:StreamingContext,lines:DStream[String]){
     //Define Keyspace
    val keyspaceName ="lambdaarchitecture"
    //Define table to persisting process records
    val csRealTimeTableName="realtimedata"
    
    lines.foreachRDD { 
      x => 
      //Splitting, flattening and finally filtering to exclude any Empty Rows
      val rawCrimeRDD = x.map(_.split("\n")).flatMap { x => x }.filter { x => x.length()>2 }
      println("Real Time Data Received = "+rawCrimeRDD.collect().length)
      //Splitting again for each Distinct value in the Row
      val splitCrimeRDD = rawCrimeRDD.map { x => x.split(",") }
      //Converting RDD of String to Objects [Crime] 
      val crimeRDD = splitCrimeRDD.map(c => Crime(c(0), c(1),c(2),c(3),c(4),c(5),c(6)))
       val sqlCtx = getInstance(streamCtx.sparkContext)
       //Using Dynamic Mapping for creating DF 
      import sqlCtx.implicits._
      //Converting RDD to DataFrame
      val dataFrame = crimeRDD.toDF()
      //Perform few operations on DataFrames
      println("Number of Rows in Data Frame = "+dataFrame.count())
      
      // Perform Group By Operation using Raw SQL
      val rtViewFrame = dataFrame.groupBy("primarytype").count()
      //Adding a new column to DF for PK
      val finalRTViewFrame = rtViewFrame.withColumn("id", new Column("count")+System.nanoTime)
      //Printing the records which will be persisted in Cassandra
      println("showing records which will be persisted into the realtime view")
      finalRTViewFrame.show(10)
      
      //Leveraging the DF.save for persisting/ Appending the complete DataFrame.
      finalRTViewFrame.write.format("org.apache.spark.sql.cassandra").
      options(Map( "table" -> "realtimedata", "keyspace" -> "lambdaarchitecture" )).
      mode(SaveMode.Append).save()
      
    }    
  }
  
  //Defining Singleton SQLContext variable
  @transient private var instance: SQLContext = null
  //Lazy initialization of SQL Context
  def getInstance(sparkContext: SparkContext): SQLContext =
    synchronized {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  
  
}

// Define the schema using a case class.
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface.
  //ID,Case Number,Date,Block,IUCR,Primary Type,Description
  case class Crime(id: String, caseNumber:String, date:String, block:String, IUCR:String, primarytype:String, desc:String)


