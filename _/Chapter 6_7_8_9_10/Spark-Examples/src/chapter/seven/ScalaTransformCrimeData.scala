package chapter.seven

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.hadoop._
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._


/**
 * Transformation Job for showcasing different transformations on the Crime Dataset.
 * @author Sumit Gupta
 *
 */
object ScalaTransformCrimeData {

  def main(args: Array[String]) {
    println("Creating Spark Configuration")
    //Create an Object of Spark Configuration
    val conf = new SparkConf()
    //Set the logical and user defined Name of this Application
    conf.setAppName("Scala - Transforming Crime Dataset")
    println("Creating Spark Context")
    //Create a Spark Context and provide previously created
    //Object of SparkConf as an reference.
    val ctx = new SparkContext(conf)
    //Define the location of the file containing the Crime Data
    val file = "file:///home/ec2-user/softwares/crime-data/Crimes_-Aug-2015.csv";
    println("Loading the Dataset and will further process it")
    //Loading the Text file from the local file system or HDFS
    //and converting it into RDD.
    //SparkContext.textFile(..) - It uses the Hadoop's
    //TextInputFormat and file is broken by New line Character.
    //Refer to http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapred/TextInputFormat.html
    //The Second Argument is the Partitions which specify the parallelism.
    //It should be equal or more then number of Cores in the cluster.
    val logData = ctx.textFile(file, 2)

    //Now Perform the Transformations on the Data Loaded by Spark
    executeTransformations(ctx, logData)
    //Stop the Context for Graceful Shutdown
    ctx.stop()

  }

  /**
   * Main Function for invoking all kind of Transformation on Crime Data
   */
  def executeTransformations(ctx: SparkContext, crimeData: RDD[String]) {

    //Provide the Count of All Crimes by its "Primary Type"
    findCrimeCountByPrimaryType(ctx, crimeData)
    //Find the Top 5 Crimes by its "Primary Type"
    findTop5Crime(ctx, crimeData)
    //Provide Custom Sorting on the type of Crime "Primary Type"
    performSortOnCrimeType(ctx, crimeData)
    //Persisting Crime Data in Text file, Object file and HDFS
    persistCrimeData(ctx, crimeData)
    //Join the 2 datasets and then get the Crime Count by IUCR Codes
    findCrimeCountByIUCRCodes(ctx, crimeData)

  }

  /**
   * Provide the Count of All Crimes by its "Primary Type"
   */

  def findCrimeCountByPrimaryType(ctx: SparkContext, crimeData: RDD[String]) {

    //Utility class for Transforming Crime Data
    val analyzer = new ScalaCrimeUtil()

    //Flattening the Crime Data by converting into Map of Key/ Value Pair 
    val crimeMap = crimeData.flatMap(x => analyzer.createDataMap(x))

    //Performing 3 Steps: -
    //1. Filtering the Data and fetching data only for "Primary Type"
    //2. Creating a Map of Key Value Pair
    //3. Applying reduce function for getting count of each key 
    val results = crimeMap.filter(f => f._1.equals("Primary Type")).
      map(x => (x._2, 1)).reduceByKey(_ + _)

    //Printing the unsorted results on the Console
    println("Printing the Count by the Type of Crime")
    results.collect().foreach(f => println(f._1 + "=" + f._2))

  }

  /**
   * Find the Top 5 Crimes by its "Primary Type"
   */
  def findTop5Crime(ctx: SparkContext, crimeData: RDD[String]) {

    //Utility class for Transforming Crime Data
    val analyzer = new ScalaCrimeUtil()

    //Flattening the Crime Data by converting into Map of Key/ Value Pair 
    val crimeMap = crimeData.flatMap(x => analyzer.createDataMap(x))

    //Performing 3 Steps: -
    //1. Filtering the Data and fetching data only for "Primary Type"
    //2. Creating a Map of Key Value Pair
    //3. Applying reduce function for getting count of each key 
    val results = crimeMap.filter(f => f._1.equals("Primary Type")).
      map(x => (x._2, 1)).reduceByKey(_ + _)

    //Perform Sort based on the Count
    val sortedResults = results.sortBy(f => f._2, false)
    //Collect the Sorted results and print the Top 5 Crime
    println("Printing Sorted Top 5 Crime based on the Primary Type of Crime")
    sortedResults.collect().take(5).foreach(f => println(f._1 + "=" + f._2))

  }

  /**
   * Provide Custom Sorting on the type of Crime "Primary Type"
   */
  def performSortOnCrimeType(ctx: SparkContext, crimeData: RDD[String]) {

    //Utility class for Transforming Crime Data
    val analyzer = new ScalaCrimeUtil()

    //Flattening the Crime Data by converting into Map of Key/ Value Pair 
    val crimeMap = crimeData.flatMap(x => analyzer.createDataMap(x))

    //Performing 3 Steps: -
    //1. Filtering the Data and fetching data only for "Primary Type"
    //2. Creating a Map of Key Value Pair
    //3. Applying reduce function for getting count of each key 
    val results = crimeMap.filter(f => f._1.equals("Primary Type")).
      map(x => (x._2, 1)).reduceByKey(_ + _)

    //Perform Custom Sort based on the Type of Crime (Primary Type)
    import scala.reflect.classTag
    val customSortedResults = results.sortBy(f => createCrimeObj(f._1, f._2), true)(CrimeOrdering, classTag[Crime])
    //Collect the Sorted results and print the Top 5 Crime
    println("Now Printing Sorted Results using Custom Sorting..............")
    customSortedResults.collect().foreach(f => println(f._1 + "=" + f._2))

  }

  /**
   * Case Class which defines the Crime Object
   */
  case class Crime(crimeType: String, count: Int)

  /**
   * Utility Function for creating Object of Class Crime
   */
  val createCrimeObj = (crimeType: String, count: Int) => {
    Crime(crimeType, count)
  }

  /**
   * Custom Ordering function which defines the Sorting behavior.
   */
  implicit val CrimeOrdering = new Ordering[Crime] {
    def compare(a: Crime, b: Crime): Int = a.crimeType.compareTo(b.crimeType)
  }
  
  /**
   * Persist the filtered Crime Data Map into various formats (Text/ Object/ HDFS)
   */
    def persistCrimeData(ctx: SparkContext, crimeData: RDD[String]) {

    //Utility class for Transforming Crime Data
    val analyzer = new ScalaCrimeUtil()

    //Flattening the Crime Data by converting into Map of Key/ Value Pair 
    val crimeMap = crimeData.flatMap(x => analyzer.createDataMap(x))

    //Performing 3 Steps: -
    //1. Filtering the Data and fetching data only for "Primary Type"
    //2. Creating a Map of Key Value Pair
    //3. Applying reduce function for getting count of each key 
    val results = crimeMap.filter(f => f._1.equals("Primary Type")).
      map(x => (x._2, 1)).reduceByKey(_ + _)
    
    println("Now Persisting as Text File")
    //Ensure that the Path on local file system exists till "output" folder. 
    results.saveAsTextFile("file:///home/ec2-user/softwares/crime-data/output/Crime-TextFile"+System.currentTimeMillis())
    println("Now Persisting as Object File")
    //Ensure that the Path on local file system exists till "output" folder.
    results.saveAsObjectFile("file:///home/ec2-user/softwares/crime-data/output/Crime-ObjFile"+System.currentTimeMillis())
   
   //Creating an Object of Hadoop Config with default Values
   val hConf = new JobConf(new org.apache.hadoop.conf.Configuration())
   
   //Defining the TextOutputFormat using old Api's available with =<0.20 
   val oldClassOutput = classOf[org.apache.hadoop.mapred.TextOutputFormat[Text,Text]]
   //Invoking Output operation to save data in HDFS using using oldApi's 
   //This method accepts following Parameters: -
   //1.Path of the File on HDFS 
   //2.Key - Class which can work with the Key  
   //3.Value - Class which can work with the Key
   //4.OutputFormat - Class needed for writing the Output in a specific Format
   //5.HadoopConfig - Object of Hadoop Config
   println("Now Persisting as Hadoop File using in Hadoop's Old API's") 
   results.saveAsHadoopFile("hdfs://localhost:9000/spark/crime-data/oldApi/Crime-"+System.currentTimeMillis(), classOf[Text], classOf[Text], oldClassOutput ,hConf )
   
   
   //Defining the TextOutputFormat using new Api's available with >0.20
   val newTextOutputFormat = classOf[org.apache.hadoop.mapreduce.lib.output.TextOutputFormat[Text, Text]]
   
   //Invoking Output operation to save data in HDFS using using new Api's 
   //This method accepts same set of parameters as "saveAsHadoopFile"
   println("Now Persisting as Hadoop File using in Hadoop's New API's")
   results.saveAsNewAPIHadoopFile("hdfs://localhost:9000/spark/crime-data/newApi/Crime-"+System.currentTimeMillis(), classOf[Text], classOf[Text], newTextOutputFormat ,hConf )
      
   }
    
  /**
   * Find the Crime Count by IUCR Codes and also display the IUCR COde Names
   */
   def findCrimeCountByIUCRCodes(ctx: SparkContext, crimeData: RDD[String]) {
     
         //Utility class for Transforming Crime Data
    val analyzer = new ScalaCrimeUtil()

    //Flattening the Crime Data by converting into Map of Key/ Value Pair 
    val crimeMap = crimeData.flatMap(x => analyzer.createDataMap(x))

    //Performing 3 Steps: -
    //1. Filtering the Data and fetching data only for "Primary Type"
    //2. Creating a Map of Key Value Pair
    //3. Applying reduce function for getting count of each key
    val results = crimeMap.filter(f => f._1.equals("IUCR")).
      map(x => (x._2, 1)).reduceByKey(_ + _)
    
    
    //Loading IUCR Codes File in Spark Memory
    val iucrFile = "file:///home/ec2-user/softwares/crime-data/IUCRCodes.txt";
    println("Loading the IUCR Dataset and will further process it")
    val iucrCodes = ctx.textFile(iucrFile, 2)
    //Convert IUCR Codes into a map of Values 
    val iucrCodeMap = iucrCodes.flatMap(x => analyzer.createIUCRDataMap(x))
    //Apply Left Outer Join to get all results from Crime RDD 
    //and matching records from IUCR RDD
    val finalResults = results.leftOuterJoin(iucrCodeMap)
    
    //Finally Print the results 
    finalResults.collect().foreach(f => println(""+f._1 + "=" + f._2))
    
   }



}

