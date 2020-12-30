package chapter.eight
import org.apache.spark.sql._
import org.apache.spark._

/**
 * Example of converting an RDD into the Dataframes 
 * using Dynamic Schema Discovery
 */
object ScalaRDDToDFDynamicSchema {
  
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
    //In our Case it is String, so it be natural order sorting.
    //Last parameter of sort() shows the complete data on Console
    //(It does not Truncate anything while printing the results)
    println("Here is the First 5 Sorted Rows by 'Primary Type'")
    crimeDF.sort("primaryType").show(5,false)
    
  }
  
  // Define the schema using a case class.
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface.
  //ID,Case Number,Date,Block,IUCR,Primary Type,Description
  case class Crime(id: String, caseNumber:String, date:String, block:String, IUCR:String, primaryType:String, desc:String)

}