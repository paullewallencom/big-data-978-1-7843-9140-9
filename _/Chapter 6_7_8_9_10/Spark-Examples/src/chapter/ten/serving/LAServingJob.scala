package chapter.ten.serving

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

/**
 * Job produce/ print the final outcome of the Lambda Architecture
 */

object LAServingJob {

  def main(args: Array[String]) {
    println("Creating Spark Configuration")
    //Create an Object of Spark Configuration
    val conf = new SparkConf()
    //Set the logical and user defined Name of this Application
    conf.setAppName("LA Serving Job")
    //Configurng Cassandra Host Name
    conf.set("spark.cassandra.connection.host", "localhost")
    println("Retreiving Spark Context from Spark Conf")
    //Retrieving Streaming Context from SparkConf Object.
    //Second parameter is the time interval at which streaming data will be divided into batches  
    val sparkCtx = new SparkContext(conf)
    //Invoking Function for generating Master Views
    generatefinalView(sparkCtx)
    
    sparkCtx.stop()
    
    
  }
  
  /**
   * Merge/ combine/ Union batch and real time views and
   * present the final dataset to the end users
   */
  def generatefinalView(sparkCtx:SparkContext){
    //Define Keyspace
    val keyspaceName ="lambdaarchitecture"
    //Define Master (Append Only) Table
    val csRealTimeTableName="realtimedata"
    //Define Table for persisting Batch View Data
    val csBatchViewTableName = "batchviewdata"
    
    // Get Instance of Spark SQL
    val sqlCtx = getInstance(sparkCtx)
    //Load the data from "batchviewdata" Table
    val batchDf  = sqlCtx.read.format("org.apache.spark.sql.cassandra")
     .options(Map( "table" -> "batchviewdata", "keyspace" -> "lambdaarchitecture" )).load()
     
    //Load the data from "realtimedata" Table
    val realtimeDF  = sqlCtx.read.format("org.apache.spark.sql.cassandra")
     .options(Map( "table" -> "realtimedata", "keyspace" -> "lambdaarchitecture" )).load()
     
     //Select only Primary Type and Count from Real Time View
     val seRealtimeDF = realtimeDF.select("primarytype", "count")
     
     //Merge/ Union both ("batchviewdata" and "realtimedata") and 
     //produce/ print the final Output on Console 
     println("Final View after merging Batch and Real-Time Views")
     val finalView = batchDf.unionAll(seRealtimeDF).groupBy("primarytype").sum("count")
     finalView.show(20)
       
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