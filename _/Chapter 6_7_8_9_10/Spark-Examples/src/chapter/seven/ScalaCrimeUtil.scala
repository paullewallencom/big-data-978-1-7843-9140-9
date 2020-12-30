package chapter.seven

class ScalaCrimeUtil extends Serializable{
  
   /**
   * Create a Map of the data which is extracted by applying Regular expression.
   */
  def createDataMap(data:String): Map[String, String] = {
    
    //Replacing Empty columns with the blank Spaces, 
    //so that split fucntion always produce same size Array
    val crimeData = data.replaceAll(",,,", ", , , ")
    //Splitting the Single Crime record
    val array = crimeData.split(",")
    //Creating the Map of values
    val dataMap = Map[String, String](
    ("ID" -> array(0)),
    ("Case Number" -> array(1)),
    ("Date" -> array(2)),
    ("Block" -> array(3)),
    ("IUCR" -> array(4)),
    ("Primary Type" -> array(5)),
    ("Description" -> array(6)),
    ("Location Description" -> array(7)),
    ("Arrest" -> array(8)),
    ("Domestic" -> array(9)),
    ("Beat" -> array(10)),
    ("District" -> array(11)),
    ("Ward" -> array(12)),
    ("Community Area" -> array(13)),
    ("FBI Code" -> array(14)),
    ("X Coordinate" -> array(15)),
    ("Y Coordinate" -> array(16)),
    ("Year" -> array(17)),
    ("Updated On" -> array(18)),
    ("Latitude" -> array(19)),
    ("Longitude" -> array(20).concat(array(21)))
    
  )
  //Finally returning it to the invoking program 
  return dataMap
  }
  
   /**
   * Create a Map of the data which is extracted by applying Regular expression.
   */
  def createIUCRDataMap(data:String): Map[String, String] = {
    
    //Replacing Empty columns with the blank Spaces, 
    //so that split function always produce same size Array
    val icurData = data.replaceAll(",,,", ", , , ")
    //Splitting the Single Crime record
    val array = icurData.split(",")
    //Creating the Map of values "IUCR Codes = Values"
    val iucrDataMap = Map[String, String](
    (array(0) -> array(1))
  )
  //Finally returning it to the invoking program 
  return iucrDataMap
  }

}