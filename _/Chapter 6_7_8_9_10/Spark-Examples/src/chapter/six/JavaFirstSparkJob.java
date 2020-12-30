package chapter.six;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class JavaFirstSparkJob {

	public static void main(String[] args) {
		System.out.println("Creating Spark Configuration");
		 // Create an Object of Spark Configuration
		 SparkConf javaConf = new SparkConf();
		 // Set the logical and user defined Name of this Application
		 javaConf.setAppName("My First Spark Java Application");
		 System.out.println("Creating Spark Context");
		 // Create a Spark Context and provide previously created
		 //Objectx of SparkConf as an reference.
		 JavaSparkContext javaCtx = new JavaSparkContext(javaConf);
		 System.out.println("Loading the Crime Dataset and will further process it");

		 String file = "file:///home/ec2-user/softwares/crime-data/Crimes_-Aug-2015.csv";
		 JavaRDD<String> logData = javaCtx.textFile(file);
		 //Invoking Filter operation on the RDD.
		 //And counting the number of lines in the Data loaded
		 //in RDD.
		 //Simply returning true as "TextInputFormat" have already divided the data by "\n"
		 //So each RDD will have only 1 line.
		 long numLines = logData.filter(new Function<String, Boolean>() {
		 public Boolean call(String s) {
		 return true;
		 }
		 }).count();
		 //Finally Printing the Number of lines
		 System.out.println("Number of Crimes reported in Aug-2015 = "+numLines);
		 javaCtx.close();

	}

}
