package chapter.six

import org.apache.spark.{SparkConf, SparkContext}

/**
 * First Spark Job to Count the number of Crimes in the given Crime Dataset
 */
object ScalaFirstSparkJob {

  def main(args: Array[String]) {
    println("Creating Spark Configuration")
   //Create an Object of Spark Configuration
   val conf = new SparkConf()
   //Set the logical and user defined Name of this Application
   conf.setAppName("My First Spark Scala Application")
   println("Creating Spark Context")
   //Create a Spark Context and provide previously created
   //Object of SparkConf as an reference.
   val ctx = new SparkContext(conf)
   //Define the location of the file contianing the Crime Data
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
   
   //Invoking Filter operation on the RDD, and counting the number of lines in the Data loaded in RDD.
   //Simply returning true as "TextInputFormat" have already divided the data by "\n"
   //So each RDD will have only 1 line.
   val numLines = logData.filter(line => true).count()
   //Finally Printing the Number of lines.
   println("Number of Crimes reported in Aug-2015 = " + numLines)
   
   //Stop the Context for Graceful Shutdown
   ctx.stop()
  }

}