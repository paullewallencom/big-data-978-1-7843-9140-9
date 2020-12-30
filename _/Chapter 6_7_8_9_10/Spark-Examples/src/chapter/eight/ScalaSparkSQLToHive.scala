package chapter.eight

import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext

/**
 * Spark SQL Job which create Tables, loads Data and execute some analytical Hive Queries.
 */
object ScalaSparkSQLToHive {
  
  def main(args:Array[String]){
    
    //Defining/ Creating SparkCOnf Object
    val conf = new SparkConf()
    //Setting Application/ Job Name
    conf.setAppName("Spark SQL - RDD To Hive")
    // Define Spark Context which we will use to initialize our SQL Context 
    val sparkCtx = new SparkContext(conf)
    //Creating Hive Context
    val hiveCtx = new HiveContext(sparkCtx)
    //Creating a Hive Tables
    println("Creating a new Hive Table - ChicagoCrimeRecordsAug15")
    hiveCtx.sql("CREATE TABLE IF NOT EXISTS ChicagoCrimeRecordsAug15(ID STRING,CaseNumber STRING, CrimeDate STRING,Block STRING,IUCR STRING,PrimaryType STRING,Description STRING,LocationDescription STRING,Arrest STRING,Domestic STRING,Beat STRING,District STRING,Ward STRING,CommunityArea STRING,FBICode STRING,XCoordinate STRING,YCoordinate STRING,Year STRING,UpdatedOn STRING,Latitude STRING,Longitude STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile")
    println("Creating a new Hive Table - icurCodes")
    hiveCtx.sql("CREATE TABLE IF NOT EXISTS iucrCodes(IUCR STRING,PRIMARY_DESC STRING ,SECONDARY_DESC STRING,INDEXCODE STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile")
    //Load the Data in Hive Table
    println("Loading Data in Hive Table - ChicagoCrimeRecordsAug15")
    hiveCtx.sql("LOAD DATA LOCAL INPATH '/home/ec2-user/softwares/crime-data/Crimes_-Aug-2015.csv' OVERWRITE INTO TABLE ChicagoCrimeRecordsAug15")
    println("Loading Data in Hive Table - iucrCodes")
    hiveCtx.sql("LOAD DATA LOCAL INPATH '/home/ec2-user/softwares/crime-data/IUCRCodes.csv' OVERWRITE INTO TABLE iucrCodes")
    //Quick Check on the number of records loaded in the Hive Table
    println("Quick Check on the Number of records Loaded in ChicagoCrimeRecordsAug15")
    hiveCtx.sql("select count(1) from ChicagoCrimeRecordsAug15").show()
    println("Quick Check on the Number of records Loaded in iucrCodes")
    hiveCtx.sql("select count(1) from iucrCodes").show()
    
    println("Now Performing Analysis")
    println("Top 5 Crimes in August Based on IUCR Codes")
    hiveCtx.sql("select B.PRIMARY_DESC, count(A.IUCR) as countIUCR from ChicagoCrimeRecordsAug15 A,iucrCodes B where A.IUCR=B.IUCR group by B.PRIMARY_DESC order by countIUCR desc").show(5)
  
    println("Count of Crimes which are of Type 'Domestic' and someone is 'Arrested' by the Police")
    hiveCtx.sql("select B.PRIMARY_DESC, count(A.IUCR) as countIUCR from ChicagoCrimeRecordsAug15 A,iucrCodes B where A.IUCR=B.IUCR and A.domestic='true' and A.arrest='true' group by B.PRIMARY_DESC order by countIUCR desc").show()
    
    println("Find Top 5 Community Areas where Highest number of Crimes have been Comitted in Aug-2015")
    hiveCtx.sql("select CommunityArea, count(CommunityArea) as cnt from ChicagoCrimeRecordsAug15 group by CommunityArea order by cnt desc").show(5)

  }
  

}