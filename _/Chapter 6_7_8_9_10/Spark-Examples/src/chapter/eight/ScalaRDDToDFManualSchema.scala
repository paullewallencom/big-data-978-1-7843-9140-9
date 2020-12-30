package chapter.eight

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark._

/**
 * Example of converting an RDD into the DataFrames 
 * using Manual Schema Creation
 */

object ScalaRDDToDFManualSchema {
  
  def main(args: Array[String]) {
    //Defining/ Creating SparkCOnf Object
    val conf = new SparkConf()
    //Setting Application/ Job Name
    conf.setAppName("Spark SQL - RDD To DataFrame - Dynamic Schema")
    // Define Spark Context which we will use to initialize our SQL Context 
    val sparkCtx = new SparkContext(conf)
    //Creating SQL Context
    val sqlCtx = new SQLContext(sparkCtx)
    
    //Define path of our Crime Data File which needs to be processed 
    val crimeDataFile = "file:///home/ec2-user/softwares/crime-data/Crimes_-Aug-2015.csv";
    // Create an RDD
    val crimeData = sparkCtx.textFile(crimeDataFile)
    
    //Assuming the Schema needs to be created from the String of Columns
    val schema = "ID,CaseNumber,Date,Block,IUCR,PrimaryType,Description"
    
    val colArray = schema.split(",")
    
    val structure = StructType(List(
                      StructField(colArray(0), StringType, true), 
                      StructField(colArray(1), StringType, true),
                      StructField(colArray(2), StringType, true),
                      StructField(colArray(3), StringType, true),
                      StructField(colArray(4), StringType, true),
                      StructField(colArray(5), StringType, true),
                      StructField(colArray(6), StringType, true)
                      ))
    
    //It can be done like this too
    //val structure = StructType(schema.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    
    //Convert records of the RDD (Crime Records) to Rows.
    val crimeRowRDD = crimeData.map(_.split(",")).map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6)))

    //Apply the schema to the RDD.
    val crimeDF = sqlCtx.createDataFrame(crimeRowRDD, structure)

    //Invoking various DataFrame Functions 
    println("Printing the Schema...")
    crimeDF.printSchema()
    //Printing Total Rows Loaded into DataFrames
    println("Total Rows - "+crimeDF.count())
    //Printing first 5 Rows of the DataFrame
    println("Here is the First 5 Row")
    crimeDF.show(5)
    //Sorting the records and then Printing First 5 Rows
    //Sorting is based on the DataType of the Column. 
    //In our Case it is String, so it be natural order sorting
    println("Here is the First 5 Sorted Rows by 'PrimaryType'")
    crimeDF.sort("PrimaryType").show(5,false)
    
  }
  
  
}