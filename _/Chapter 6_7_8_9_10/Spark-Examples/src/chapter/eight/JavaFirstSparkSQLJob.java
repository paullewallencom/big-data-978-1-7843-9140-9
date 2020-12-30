package chapter.eight;

import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;

/**
 * First Spark SQL Job, showcasing all the basic constructs for : - <br/> 
 * 1. Creating DataFrames and invoking basic Operations <br/>
 * 2. Creating Tables from DataFrames and Invoking SQL Queries <br/>
 *
 */
public class JavaFirstSparkSQLJob {

	/**
	 * Constructor
	 */
	public JavaFirstSparkSQLJob() {
		System.out.println("Creating Spark Configuration");
		// Create an Object of Spark Configuration
		SparkConf javaConf = new SparkConf();
		// Set the logical and user defined Name of this Application
		javaConf.setAppName("First Spark SQL Job in Java");
		System.out.println("Creating Spark Context");
		// Create a Spark Context and provide previously created
		// Object of SparkConf as an reference.
		JavaSparkContext javaCtx = new JavaSparkContext(javaConf);

		// Defining path of the JSON file which contains the data in JSON Format
		String jsonFile = "file:///home/ec2-user/softwares/crime-data/company.json";

		// Creating SQL Context
		SQLContext sqlContext = new SQLContext(javaCtx);
		// Utility method exposed by SQLContext for reading JSON file
		// and create dataFrame
		// Once DataFrame is created all the data names like "Name" in the JSON
		// file will be interpreted as Column Names and their data Types 
		// will be interpreted automatically based on their values
		DataFrame dataFrame = sqlContext.read().json(jsonFile);
		// Defining a function which will execute operations
		// exposed by DataFrame API's
		executeDataFrameOperations(sqlContext, dataFrame);
		// Defining a function which will execute SQL Queries using
		// DataFrame API's
		executeSQLQueries(sqlContext, dataFrame);

		//Closing Context for clean exit
		javaCtx.close();

	}

	/**
	 * This function executes various operations exposed by DataFrame API
	 */
	public void executeDataFrameOperations(SQLContext sqlCtx,
			DataFrame dataFrame) {
		// Invoking various basic operations available with DataFrame
		System.out.println("Printing the Schema...");
		dataFrame.printSchema();
		// Printing Total Rows Loaded into DataFrames
		System.out.println("Total Rows - " + dataFrame.count());
		// Printing first Row of the DataFrame
		System.out.println("Printing All Rows in the Data Frame");
		dataFrame.show();
		// Sorting the records and then Printing all Rows again
		// Sorting is based on the DataType of the Column.
		// In our Case it is String, so it be natural order sorting
		System.out.println("Here is the Sorted Rows by 'No_Of_Supervisors' - Descending");
		DataFrame sortedDF = dataFrame.sort(dataFrame.col("No_Of_Supervisors").desc());
		sortedDF.show();

	}

	/**
	 * This function registers the DataFrame as SQL Table and execute SQL
	 * Queries
	 */
	public void executeSQLQueries(SQLContext sqlCtx, DataFrame dataFrame) {

		// The first step is to register the DataFrame as temporary table
		// And give it a name. In our Case "Company"
		dataFrame.registerTempTable("Company");
		System.out.println("Executing SQL Queries...");
		// Now Execute the SQL Queries and print the results
		// Calculating the total Count of Rows
		DataFrame dfCount = sqlCtx
				.sql("select count(1) from Company");
		System.out.println("Calcualting total Rows in the Company Table...");
		dfCount.show();

		// Printing the complete data in the Company table
		DataFrame df = sqlCtx.sql("select * from Company");
		System.out.println("Dumping the complete Data of Company Table...");
		df.show();

		// Printing the complete data in the Company table sorted by Supervisors
		DataFrame dfSorted = sqlCtx
				.sql("select * from Company order by No_Of_Supervisors desc");
		System.out
				.println("Dumping the complete Data of Company Table, sorted by Supervisors - Descending...");
		dfSorted.show();

	}

	/**
	 * Main Method
	 * @param args
	 */
	public static void main(String[] args) {
		new JavaFirstSparkSQLJob();
	}

}
