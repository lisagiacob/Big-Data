package it.polito.bigdata.spark.example;

import scala.Tuple2;

import org.apache.spark.api.java.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath = "sampleData/registerSample.csv";
		String inputPath2 = "sampleData/stations.csv";
		Double threshold = 0.4;
		String outputFolder = "out";
/* 
		inputPath = args[0];
		inputPath2 = args[1];
		threshold = Double.parseDouble(args[2]);
		outputFolder = args[3];
*/
		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: "+JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");

		// Compute the criticality value for each pair (Si, Tj)
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		// Some lines are characterized by used slots = 0 and free slots = 0 and must be filtered
		JavaRDD<String> filteredRDD = inputRDD.filter(line -> {
			//Filtering the first line
			if(line.startsWith("station")) return false;
			else {
				String[] words = line.split("\t");
				// If used slots and free slots are 0....
				if(words[2]=="0" && words[3]=="0") return false;
				else return true;
			}
		});
/* 
		JavaRDD<String> criticalRDD = filteredRDD.filter(line -> {
			String[] words = line.split("\t");
			// If free slots are 0....
			if(!words[3].equals("0")) return false;
			else return true;
		}); // I get an RDD of only critical readings
*/
		//criticalRDD.saveAsTextFile(outputFolder);

		// Create a JavaPairRDD with as key "sId day hour" and as value "#free slots" for all readings
		JavaPairRDD<String, String> SenFreeRDD = filteredRDD.mapToPair(line -> {
			String[] words = line.split("\t");
			String data = words[1].split(" ")[0];
			String time = words[1].split(" ")[1];
			String hour = time.split(":")[0];
			String key = words[0] + " " + DateTool.DayOfTheWeek(data) + " " + hour;
			String fS = "1 0"; //primo val a 1 perchè indica che è una linea gen e secondo a 0 perchè non va contata con le linee critiche
			if(words[3].equals("0")) fS = "1 1"; //perchè va fatto +1 sia per gen sia per crit
			return new Tuple2<String, String>(key,fS);
		});

		// JavaPairRDD with each key and the numberds of criticality it has
		// Shouldnt be too large since i only consider the day and the time it was critical
		JavaPairRDD<String, String> senTotCritRDD = SenFreeRDD.reduceByKey( (val1, val2) -> {
			int tot = Integer.parseInt(val1.split(" ")[0]) + Integer.parseInt(val2.split(" ")[0]);
			int crit = Integer.parseInt(val1.split(" ")[1]) + Integer.parseInt(val2.split(" ")[1]); 
			String totCrit = tot + " " + crit;
			return totCrit;
		});

		// Computes the criticality value for each pair (Si,Tj).
		JavaPairRDD<String, Double> senCritRDD = senTotCritRDD.mapToPair(line -> {
			String St = line._1();
			Double crit = Double.parseDouble(line._2().split(" ")[1])/Double.parseDouble(line._2.split(" ")[0]);
			return new Tuple2<String, Double>(St, crit);
		});

		// Selects only the pairs having a criticality value greater than or equal to a minimum criticality 
		// threshold. The minimum criticality threshold is an argument of the application.
		JavaPairRDD<String, Double> HThreRDD = senCritRDD.filter(line -> {
			if(line._2() > threshold) return true;
			else return false;
		});

		// Selects the most critical timeslot for each station (consider only timeslots with a criticality 
		// greater than or equal to the minimum criticality threshold). If there are two or more timeslots 
		// characterized by the highest criticality value for the same station, select only one of those 
		// timeslots. Specifically, select the one associated with the earliest hour. 
		// If also the hour is the same, consider the lexicographical order of the name of the week day.
		JavaPairRDD<String, String> critTimeRDD = senCritRDD.mapToPair(line -> {
			String key = line._1().split(" ")[0];
			String dayHourVal = line._1().split(" ")[1] + " - " + line._1().split(" ")[2] + " - " + line._2();
			return new Tuple2<String, String>(key, dayHourVal);
		});
		
		JavaPairRDD<String, String> senTRDD = critTimeRDD.reduceByKey((val1, val2) -> {
			Double crit1 = Double.parseDouble(val1.split(" - ")[2]);
			Double crit2 = Double.parseDouble(val2.split(" - ")[2]);
			Integer hour1 = Integer.parseInt(val1.split(" - ")[1]);
			Integer hour2 = Integer.parseInt(val2.split(" - ")[1]);
			String day1 = val1.split(" - ")[0];
			String day2 = val2.split(" - ")[0];
			// max funtion
			if(crit1 > crit2) return day1 + " - " + hour1 + " - " + crit1;
			else if(crit1 < crit2) return day2 + " - " + hour2 + " - " + crit2;
			else {
				if(hour1 < hour2) return day1 + " - " + hour1 + " - " + crit1;
				else if(hour1 > hour2) return day2 + " - " + hour2 + " - " + crit2;
				else {
					if (day1.compareTo(day2) < 0) return day1 + " - " + hour1 + " - " + crit1;
					else return day2 + " - " + hour2 + " - " + crit2;
				}
			}
		});

		/**
		 * Stores in one single (KML) file the information about the most critical timeslot for each station. 
		 * Specifically, the output (KML) file must contain one marker of type Placemark for each pair 
		 * (Si, most critical timeslot for Si) characterized by the following features:
			o StationId
			o Day of the week and hour of the critical timeslot
			o Criticality value
			o Coordinates of the station (longitude, latitude)
		 * Do not include in the output (KML) file the stations for which there are no timeslots satisfying the 
		 * minimum criticality threshold.
		*/

		// Read the location of each input station
		JavaPairRDD<String, String> stationLocation = sc.textFile(inputPath2).mapToPair(line -> {
			// id latitude longitude name
			// 1 41.397978 2.180019 Gran Via Corts Catalanes
			String[] fields = line.split("\\t");

			return new Tuple2<String, String>(fields[0], fields[1] + "," + fields[2]);
		});

		JavaPairRDD<String, Tuple2<String, String>> resultLocations = senTRDD.join(stationLocation);
		JavaRDD<String> resultKML = resultLocations
		.map((Tuple2<String, Tuple2<String, String>> StationMax) -> {

			String stationId = StationMax._1();

			String dWHC = StationMax._2()._1();
			String coordinates = StationMax._2()._2();

			String result = "<Placemark><name>" + stationId + "</name>" + "<ExtendedData>"
					+ "</ExtendedData>" + "<Point>" + "<coordinates>" + coordinates + "</coordinates>"
					+ "</Point>" + "</Placemark>";

			return result;
		});

		// Invoke coalesce(1) to store all data inside one single partition/i.e., in one single output part file
		resultKML.coalesce(1).saveAsTextFile(outputFolder); 

		// Close the Spark context
		sc.close();
	}
}
