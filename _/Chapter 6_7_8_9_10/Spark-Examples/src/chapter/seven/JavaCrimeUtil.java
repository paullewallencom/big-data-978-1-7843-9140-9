package chapter.seven;

import java.io.*;
import java.util.*;

import scala.Tuple2;

public class JavaCrimeUtil implements Serializable{
	  
	
	
	
	   /**
	   * Create a Map of the data which is extracted by applying Regular expression.
	   */
	  public List<scala.Tuple2<String, String>> createDataMap(String data){
	    
		 List<Tuple2<String, String>> pairs = new LinkedList<>();
		  
	    //Replacing Empty columns with the blank Spaces, 
	    //so that split function always produce same size Array
	    String crimeData = data.replaceAll(",,,", ", , , ");
	    //Splitting the Single Crime record
	    String[] array = crimeData.split(",");
	    //Creating the Map of values
	   if(!array[0].equalsIgnoreCase("ID")){
	    pairs.add(new scala.Tuple2<>("ID",array[0]));
	    pairs.add(new scala.Tuple2<>("Case Number", array[1]));
	    pairs.add(new scala.Tuple2<>("Date" ,array[2]));
	    pairs.add(new scala.Tuple2<>("Block" , array[3]));
	    pairs.add(new scala.Tuple2<>("IUCR" , array[4]));
	    pairs.add(new scala.Tuple2<>("Primary Type" , array[5]));
	    pairs.add(new scala.Tuple2<>("Description" , array[6]));
	    pairs.add(new scala.Tuple2<>("Location Description" , array[7]));
	    pairs.add(new scala.Tuple2<>("Arrest" , array[8]));
	    pairs.add(new scala.Tuple2<>("Domestic" , array[9]));
	    pairs.add(new scala.Tuple2<>("Beat" , array[10]));
	    pairs.add(new scala.Tuple2<>("District" , array[11]));
	    pairs.add(new scala.Tuple2<>("Ward" , array[12]));
	    pairs.add(new scala.Tuple2<>("Community Area" , array[13]));
	    pairs.add(new scala.Tuple2<>("FBI Code" , array[14]));
	    pairs.add(new scala.Tuple2<>("X Coordinate" , array[15]));
	    pairs.add(new scala.Tuple2<>("Y Coordinate" , array[16]));
	    pairs.add(new scala.Tuple2<>("Year" , array[17]));
	    pairs.add(new scala.Tuple2<>("Updated On" , array[18]));
	    pairs.add(new scala.Tuple2<>("Latitude" , array[19]));
	    pairs.add(new scala.Tuple2<>("Longitude", array[20].concat(array[21])));
	   }
	  
	  //Finally returning it to the invoking program 
	  return pairs;
	  }
	  
	   /**
	   * Create a Map of the data which is extracted by applying Regular expression.
	   */
	  public  Map<String, String> createIUCRDataMap(String data) {
	    
			    //Replacing Empty columns with the blank Spaces, 
			    //so that split fucntion always produce same size Array
			    String crimeData = data.replaceAll(",,,", ", , , ");
			    //Splitting the Single Crime record
			    String[] array = crimeData.split(",");
			    //Creating the Map of values
			    Map <String,String> iucrDataMap = new HashMap<String, String>();
			    iucrDataMap.put(array[0] , array[1]);
			    
	  //Finally returning it to the invoking program 
	  return iucrDataMap;
	  }

	}