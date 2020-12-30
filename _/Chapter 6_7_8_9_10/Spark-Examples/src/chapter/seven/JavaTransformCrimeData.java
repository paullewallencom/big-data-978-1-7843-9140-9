package chapter.seven;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.*;
import java.io.*;

/**
 * Java Implementation of Transformation 
 * @author Sumit Gupta
 *
 */
public class JavaTransformCrimeData implements Serializable{

	/**
	 * Constructor for initializing the Transformation Job
	 */
	public JavaTransformCrimeData() {
		System.out.println("Creating Spark Configuration");
		// Create an Object of Spark Configuration
		SparkConf javaConf = new SparkConf();
		// Set the logical and user defined Name of this Application
		javaConf.setAppName("Java - Transforming Crime Dataset");
		System.out.println("Creating Spark Context");
		// Create a Spark Context and provide previously created
		// Object of SparkConf as an reference.
		JavaSparkContext javaCtx = new JavaSparkContext(javaConf);
		System.out
				.println("Loading the Crime Dataset and will further process it");

		String file = "file:///home/ec2-user/softwares/crime-data/Crimes_-Aug-2015.csv";
		JavaRDD<String> logData = javaCtx.textFile(file);

		executeTransformations(javaCtx, logData);

		javaCtx.close();

	}

	public static void main(String[] args) {

		new JavaTransformCrimeData();
	}

	/**
	 * Main Function for invoking all kind of Transformation on Crime Data
	 */
	private void executeTransformations(JavaSparkContext ctx,
			JavaRDD<String> crimeData) {

		// Provide the Count of All Crimes by its "Primary Type"
		findCrimeCountByPrimaryType(ctx, crimeData);
		// Find the Top 5 Crimes by its "Primary Type"
		findTop5Crime(ctx, crimeData);
	}

	/**
	 * Provide the Count of All Crimes by its "Primary Type"
	 * @param ctx
	 * @param crimeData
	 */
	private void findCrimeCountByPrimaryType(JavaSparkContext ctx,
			JavaRDD<String> crimeData) {
		
		
		//Flattening the Crime Data by converting into Map of Key/ Value Pair
		JavaPairRDD<String,String> crimeDataMap = 
				crimeData.flatMapToPair(new PairFlatMapFunction<String,String,String>(){
					@Override
					public Iterable <scala.Tuple2<String,String>> call(String data){
						JavaCrimeUtil util = new JavaCrimeUtil();
						List<Tuple2<String, String>> pairs = util.createDataMap(data);
						return pairs;
						
					}
				}
			);
		
		//Filtering Based on Primary Type
		JavaPairRDD<String,String> crimeDataFiltered = crimeDataMap.filter(new Function<Tuple2<String,String>,Boolean>(){
			 @Override
	          public Boolean call(Tuple2<String,String> data) {
				 if(data._1.equalsIgnoreCase("Primary Type")){
					 return  true;
				 }
				 return false;
	            
	          }
		});
		
		//Converting and reversing the values of Map <String, Integer> 
		JavaPairRDD<String,Integer> crimeDataFilteredMap = 
				crimeDataFiltered.mapToPair(new PairFunction<Tuple2<String,String>,String,Integer>(){
					
					@Override
					public Tuple2<String,Integer> call(Tuple2<String,String> data){
						return new Tuple2<String,Integer>(data._2,1);
					}
				});
				
		//Finally Invoking reduce Function for getting the Final Count
		JavaPairRDD<String,Integer> crimeDataFilteredMapCount =crimeDataFilteredMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
	        @Override
	        public Integer call(Integer a, Integer b) {
	         return a + b;
	        }
	    });
			
		System.out.println("Here is the results...........");
		System.out.println("Crime Data ..........."+crimeData.count());
		System.out.println("Crime Data Map ..........."+crimeDataMap.collect().size());
		System.out.println("Final Crime Data Map Count..........."+ crimeDataFilteredMapCount.collectAsMap());
	
	}

	/**
	 * Computing Top 5 Crimes 
	 * @param ctx
	 * @param crimeData
	 */
	private void findTop5Crime(JavaSparkContext ctx, JavaRDD<String> crimeData) {
		//Flattening the Crime Data by converting into Map of Key/ Value Pair
		JavaPairRDD<String,String> crimeDataMap = 
				crimeData.flatMapToPair(new PairFlatMapFunction<String,String,String>(){
					@Override
					public Iterable <scala.Tuple2<String,String>> call(String data){
						JavaCrimeUtil util = new JavaCrimeUtil();
						List<Tuple2<String, String>> pairs = util.createDataMap(data);
						return pairs;
						
					}
				}
			);
		
		//Filtering Based on Primary Type
		JavaPairRDD<String,String> crimeDataFiltered = crimeDataMap.filter(new Function<Tuple2<String,String>,Boolean>(){
			 @Override
	          public Boolean call(Tuple2<String,String> data) {
				 if(data._1.equalsIgnoreCase("Primary Type")){
					 return  true;
				 }
				 return false;
	            
	          }
		});
		
		//Converting and reversing the values of Map <String, Integer> 
		JavaPairRDD<String,Integer> crimeDataFilteredMap = 
				crimeDataFiltered.mapToPair(new PairFunction<Tuple2<String,String>,String,Integer>(){
					
					@Override
					public Tuple2<String,Integer> call(Tuple2<String,String> data){
						return new Tuple2<String,Integer>(data._2,1);
					}
				});
				
		//Finally Invoking reduce Function for getting the Final Count
		JavaPairRDD<String,Integer> crimeDataFilteredMapCount =crimeDataFilteredMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
	        @Override
	        public Integer call(Integer a, Integer b) {
	         return a + b;
	        }
	    });
		
		//Converting and reversing the values of Map <String, Integer> 
		JavaPairRDD<Integer,String> crimeDataFilteredMapSorted = 
				crimeDataFilteredMapCount.mapToPair(new PairFunction<Tuple2<String,Integer>,Integer,String>(){
					@Override
					public Tuple2<Integer,String> call(Tuple2<String,Integer> data){
						return new Tuple2<Integer,String>(data._2,data._1);
					}
				}).sortByKey(false);
	
		System.out.println("Here is the results...........");
		System.out.println("Crime Data ..........."+crimeData.count());
		System.out.println("Crime Data Map ..........."+crimeDataMap.collect().size());
		System.out.println("Final Crime Data Sorted Map Count Top 5 ..........."+ crimeDataFilteredMapSorted.take(5));
	
	
	}

}
