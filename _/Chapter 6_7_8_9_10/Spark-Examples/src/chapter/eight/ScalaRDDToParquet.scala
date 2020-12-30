package chapter.eight

import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.hive._

/**
 * Reading and Writing Parquet Formats using SQLContext and HiveContext
 */
object ScalaRDDToParquet {
  
  /**
   * Main Method
   */
  def main(args:Array[String]){
        
    //Defining/ Creating SparkCOnf Object
    val conf = new SparkConf()
    //Setting Application/ Job Name
    conf.setAppName("Spark SQL - RDD To Parquet")
    // Define Spark Context which we will use to initialize our SQL Context 
    val sparkCtx = new SparkContext(conf)
    //Works with Parquet using SQLContext
    parquetWithSQLCtx(sparkCtx)
    //Works with Parquet using HiveContext
    parquetWithHiveCtx(sparkCtx)
     
  }
  //Define the schema using a case class.
  //Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  //you can use custom classes that implement the Product interface.
  //ID,Case Number,Date,Block,IUCR,Primary Type,Description
  case class Crime(id: String, caseNumber:String, date:String, block:String, IUCR:String, primaryType:String, desc:String)
  
  /**
   * Write and Read Chicago Crime data in Parquet format
   * using SQLContext
   */
  def parquetWithSQLCtx(sparkCtx:SparkContext){
    //Creating SQL Context
    val sqlCtx = new SQLContext(sparkCtx)
   
    //Define path of our Crime Data File which needs to be processed 
    val crimeData = "file:///home/ec2-user/softwares/crime-data/Crimes_-Aug-2015.csv";
    //this is used to implicitly convert an RDD to a DataFrame.
    import sqlCtx.implicits._
    //Load the data from Text File 
    val rawCrimeRDD = sparkCtx.textFile(crimeData)
    //As data is in CSV format so first step is to Split it
    val splitCrimeRDD = rawCrimeRDD.map(_.split(","))
    //Next Map the Splitted RDD to the Crime Object
    val crimeRDD = splitCrimeRDD.map(c => Crime(c(0), c(1),c(2),c(3),c(4),c(5),c(6)))
    //Invoking Implicit function to create DataFrame from RDD
    val crimeDF = crimeRDD.toDF()
      
    //Persisting Chicago Crime Data in the Spark SQL Memory by name of "ChicagoCrime.parquet"
    //In this below operation we are also using "mode" provides the instruction 
    //to Overwrite the data in case it already exist by that name 
    crimeDF.write.mode("overwrite").parquet("ChicagoCrime.parquet")
    //Now Read and print the count of Rows and Structure of Parquet tables
    val parquetDataFrame = sqlCtx.read.parquet("ChicagoCrime.parquet")
    println("Count of Rows in Parquet Table = "+parquetDataFrame.count())
    parquetDataFrame.printSchema()

    //Another way of writing and reading the Data in Parquet Format
    //crimeDF.write.format("parquet").mode("overwrite").save("ChicagoCrime.parquet")
    //val parquetDataFrame = sqlCtx.read.format("parquet").load("ChicagoCrime.parquet")
    //we can register the parquet tables as temporary Tables too
    //parquetDataFrame.registerTempTable("parquetTemporaryTable")
  }  
  
  /**
   * Write and Read Chicago Crime data in Parquet format
   * using HiveContext
   */
  def parquetWithHiveCtx(sparkCtx:SparkContext){
    //Creating Hive Context
    val hiveCtx = new HiveContext(sparkCtx)
   
    //Define path of our Crime Data File which needs to be processed 
    val crimeData = "file:///home/ec2-user/softwares/crime-data/Crimes_-Aug-2015.csv";
    //this is used to implicitly convert an RDD to a DataFrame.
    import hiveCtx.implicits._
    //Load the data from Text File 
    val rawCrimeRDD = sparkCtx.textFile(crimeData)
    //As data is in CSV format so first step is to Split it
    val splitCrimeRDD = rawCrimeRDD.map(_.split(","))
    //Next Map the Splitted RDD to the Crime Object
    val crimeRDD = splitCrimeRDD.map(c => Crime(c(0), c(1),c(2),c(3),c(4),c(5),c(6)))
    //Invoking Implicit function to create DataFrame from RDD
    val crimeDF = crimeRDD.toDF()
      
    //Persisting Chicago Crime Data in the HDFS by name of "ChicagoCrimeParquet" as a Table
    //We are also using "mode" which provides the instruction 
    //to "Overwrite" the data in case it already exist by that name 
    crimeDF.write.mode("overwrite").format("parquet").saveAsTable("ChicagoCrimeParquet")
    //Persisting Chicago Crime Data in the HDFS by name of "ChicagoCrime.parquet" in a 
    //path/ directory that already exists on HDFS.
    crimeDF.write.mode("overwrite").format("parquet").save("/spark/sql/hiveTables/parquet/")
    
    //Read and print the Parquet tables from the HDFS    
    val parquetDFTable = hiveCtx.read.format("parquet").table("ChicagoCrimeParquet")
    println("Count of Rows in Parquet Table = "+parquetDFTable.count())
    println("Printing Schema of Parquet Table")
    parquetDFTable.printSchema()    

    //Read the Parquet data from the Specified path on HDFS
    val parquetDFPath = hiveCtx.read.format("parquet").load("/spark/sql/hiveTables/parquet/")
    println("Count of Rows in Parquet Table, Loaded from HDFS Path = "+parquetDFPath.count())
    println("Printing Schema of Parquet Table, Loaded from HDFS Path")
    parquetDFPath.printSchema()

  }
    
 }  
