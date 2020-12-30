package chapter.eight

import org.apache.spark.sql._
import org.apache.spark._

/**
 * First Spark SQL Job, showcasing all the basic constructs for : -
 * 1. Creating DataFrames and invoking basic Operations
 * 2. Creating Tables from DataFrames and Invoking SQL Queries 
 */
object ScalaFirstSparkSQLJob {

  def main(args: Array[String]) {
      //Defining/ Creating SparkCOnf Object
      val conf = new SparkConf()
      //Setting Application/ Job Name
      conf.setAppName("First Spark SQL Job in Scala")
      // Define Spark Context which we will use to initialize our SQL Context 
      val sparkCtx = new SparkContext(conf)
      //Creating SQL Context
      val sqlCtx = new SQLContext(sparkCtx)
  
      //Defining path of the JSON file which contains the data in JSON Format 
      val jsonFile = "file:///home/ec2-user/softwares/crime-data/company.json" 
      
      //Utility method exposed by SQLContext for reading JSON file 
      //and create dataFrame
      //Once DataFrame is created all the data names like "Name" in the JSON file 
      //will interpreted as Column Names and their data Types will be 
      //interpreted automatically based on their values
      val dataFrame = sqlCtx.read.json(jsonFile)
    
      //Defining a function which will execute operations 
      //exposed by DataFrame API's
      executeDataFrameOperations(sqlCtx,dataFrame)
      //Defining a function which will execute SQL Queries using 
      //DataFrame API's
      executeSQLQueries(sqlCtx,dataFrame)
      
    
  }
  
  /**
   * This function executes various operations exposed by DataFrame API
   */
  def executeDataFrameOperations(sqlCtx:SQLContext, dataFrame:DataFrame):Unit = {
      //Invoking various basic operations available with DataFrame 
      println("Printing the Schema...")
      dataFrame.printSchema()
      //Printing Total Rows Loaded into DataFrames
      println("Total Rows - "+dataFrame.count())
      //Printing first Row of the DataFrame
      println("Printing All Rows in the Data Frame")
      println(dataFrame.collect().foreach { row => println(row) })
      //Sorting the records and then Printing all Rows again
      //Sorting is based on the DataType of the Column. 
      //In our Case it is String, so it be natural order sorting
      println("Here is the Sorted Rows by 'No_Of_Supervisors' - Descending")
      dataFrame.sort(dataFrame.col("No_Of_Supervisors").desc).show(10)
      
      

  }
  
  /**
   * This function registers the DataFrame as SQL Table and execute SQL Queries
   */
  def executeSQLQueries(sqlCtx:SQLContext, dataFrame:DataFrame):Unit = {
    
    //The first step is to register the DataFrame as temporary table
    //And give it a name. In our Case "Company"
    dataFrame.registerTempTable("Company")
    println("Executing SQL Queries...")
    //Now Execute the SQL Queries and print the results
    //Calculating the total Count of Rows
    val dfCount = sqlCtx.sql("select count(1) from Company")
    println("Calcualting total Rows in the Company Table...")
    dfCount.collect().foreach(println)
   
    //Printing the complete data in the Company table
    val df = sqlCtx.sql("select * from Company")
    println("Dumping the complete Data of Company Table...")
    dataFrame.collect().foreach(println)
    
    //Printing the complete data in the Company table sorted by Supervisors
    val dfSorted = sqlCtx.sql("select * from Company order by No_Of_Supervisors desc")
    println("Dumping the complete Data of Company Table, sorted by Supervisors - Descending...")
    dfSorted.collect().foreach(println)
        
  }

}