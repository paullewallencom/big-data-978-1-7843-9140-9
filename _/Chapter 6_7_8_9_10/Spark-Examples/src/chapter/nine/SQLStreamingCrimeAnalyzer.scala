package chapter.nine

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd._
import org.apache.spark.streaming.dstream._

object SQLStreamingCrimeAnalyzer {

  def main(args: Array[String]) {
    println("Creating Spark Configuration")
    //Create an Object of Spark Configuration
    val conf = new SparkConf()
    //Set the logical and user defined Name of this Application
    conf.setAppName("Our SQL Streaming Crime Analyzer in Scala")

    println("Retreiving Streaming Context from Spark Conf")
    //Retrieving Streaming Context from SparkConf Object.
    //Second parameter is the time interval at which streaming data will be divided into batches  
    val streamCtx = new StreamingContext(conf, Seconds(6))

    //Define the the type of Stream. Here we are using TCP Socket as text stream, 
    //It will keep watching for the incoming data from a specific machine (localhost) and port (provided as argument) 
    //Once the data is retrieved it will be saved in the memory and in case memory
    //is not sufficient, then it will store it on the Disk
    //It will further read the Data and convert it into DStream
    val lines = streamCtx.socketTextStream("localhost", args(0).toInt, MEMORY_AND_DISK_SER_2)
    
    lines.foreachRDD { 
      x => 
      //Splitting, flattening and finally filtering to exclude any Empty Rows
      val rawCrimeRDD = x.map(_.split("\n")).flatMap { x => x }.filter { x => x.length()>2 }
      println("Data Received = "+rawCrimeRDD.collect().length)
      //Splitting again for each Distinct value in the Row
      val splitCrimeRDD = rawCrimeRDD.map { x => x.split(",") }
      //Finally mapping/ creating/ populating the Crime Object with the values
      val crimeRDD = splitCrimeRDD.map(c => Crime(c(0), c(1),c(2),c(3),c(4),c(5),c(6)))
      //Getting instance of SQLContext and also importing implicits
      //for dynamically creating Data Frames
      val sqlCtx = getInstance(streamCtx.sparkContext)
      import sqlCtx.implicits._
      //COnverting RDD to DataFrame
      val dataFrame = crimeRDD.toDF()
      //Perform few operations on DataFrames
      println("Number of Rows in Table = "+dataFrame.count())
      println("Printing All Rows")
      dataFrame.show(dataFrame.count().toInt)
      //Now Printing Crimes Grouped by "Primary Type"
      println("Printing Crimes, Grouped by Primary Type")
      dataFrame.groupBy("primaryType").count().sort($"count".desc).show(5)
      //Now registering it as Table and and Invoking few SQL Operations
      //First step is to have Unique table Names so that it does not get overwritten and is persisted.
      //we can further create the meta data for these tables or 
      //may be have a Partioned hive table to persists the results for each time Window
      val tableName ="ChicagoCrimeData"+System.nanoTime()
      dataFrame.registerTempTable(tableName)
      invokeSQLOperation(streamCtx.sparkContext,tableName)
    }
    
    //Most important statement which will initiate the Streaming Context
    streamCtx.start();
    //Wait till the execution is completed.
    streamCtx.awaitTermination();  

    
  }
  
  def invokeSQLOperation(sparkCtx:SparkContext,tableName:String){
    println("Now executing SQL Queries.....")
    val sqlCtx = getInstance(sparkCtx)
    println("Printing the Schema...")
    sqlCtx.sql("describe "+tableName).collect().foreach { println }
    println("Printing Total Number of records.....")
    sqlCtx.sql("select count(1) from "+tableName).collect().foreach { println }

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
  case class Crime(id: String, caseNumber:String, date:String, block:String, IUCR:String, primaryType:String, desc:String)

