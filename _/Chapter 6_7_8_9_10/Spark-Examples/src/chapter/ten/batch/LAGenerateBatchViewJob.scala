package chapter.ten.batch

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnector

object LAGenerateBatchViewJob {

  def main(args: Array[String]) {
    println("Creating Spark Configuration")
    //Create an Object of Spark Configuration
    val conf = new SparkConf()
    //Set the logical and user defined Name of this Application
    conf.setAppName("LA Generate Batch View Job")
    //Configurng Cassandra Host Name
    conf.set("spark.cassandra.connection.host", "localhost")
    println("Retreiving Spark Context from Spark Conf")
    //Retrieving Streaming Context from SparkConf Object.
    //Second parameter is the time interval at which streaming data will be divided into batches  
    val sparkCtx = new SparkContext(conf)
    //Invoking Function for generating Master Views
    generateMasterView(sparkCtx)
    
    sparkCtx.stop()
    
    
  }
  
  def generateMasterView(sparkCtx:SparkContext){
    //Define Keyspace
    val keyspaceName ="lambdaarchitecture"
    //Define Master (Append Only) Table
    val csMasterTableName="masterdata"
    //Define Real Time Table
    val csRealTimeTableName="realtimedata"
    //Define Table for persisting Batch View Data
    val csBatchViewTableName = "batchviewdata"
    
    // Get Instance of Spark SQL
    val sqlCtx = getInstance(sparkCtx)
    //Load the data from "masterdata" Table
     val df  = sqlCtx.read.format("org.apache.spark.sql.cassandra")
     .options(Map( "table" -> "masterdata", "keyspace" -> "lambdaarchitecture" )).load()
     //Applying standard DataFrame fucniton for
     //performing grouping of Crime Data by Primary Type. 
     val batchView = df.groupBy("primarytype").count()
     //Persisting the grouped data into the Batch View Table
     batchView.write.format("org.apache.spark.sql.cassandra").
     options(Map( "table" -> "batchviewdata", "keyspace" -> "lambdaarchitecture" )).mode(SaveMode.Overwrite).save()
     
     //Delete the Data from Real-Time Table as now it is 
     //already part of grouping done in previous steps 
     val csConnector = CassandraConnector.apply(sparkCtx.getConf)
     val csSession = csConnector.openSession()
     csSession.execute("TRUNCATE "+keyspaceName+"."+csRealTimeTableName)
     csSession.close()
     println("Data Persisted in the Batch View Table - lambdaarchitecture.batchviewdata")
    
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