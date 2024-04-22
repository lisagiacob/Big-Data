package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath;
		String outputPath;
		
		inputPath = "input";
		outputPath = "out";

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #5")
			.setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: "+JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");
		
		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data 
		// (i.e, one pair "word\tfreq")  
		JavaRDD<String> sensorRDD = sc.textFile(inputPath);

		/** Ex. 39 Bis: Critical dates analysis
		 * Input: a textual csv file containing the daily value of PM10 for a set of sensors
		 * 	Each line of the files has the following format: sensorId, date, PM10 value (μg/m3)
		 * Output: an HDFS file containing one line for each sensor
		 * 	Each line contains a sensorId and the list of dates with a PM10
		 * 	value greater than 50 for that sensor
		 * 	Also the sensors which have never been associated with a PM10 values greater than 50 must be 
		 * 	included in the result (with an empty set)
		 */
		JavaRDD<String> filRDD = sensorRDD.filter(line -> Double.parseDouble(line.split(",")[2]) > 50);
		 
		JavaPairRDD<String, String> senDateRDD = filRDD.mapToPair(line -> new Tuple2<String, String>(line.split(",")[0], line.split(",")[1]));
		
		JavaPairRDD<String, Iterable<String>> senListRDD = senDateRDD.groupByKey();

		JavaPairRDD<String, Iterable<String>> genRDD = sensorRDD.mapToPair(line -> new Tuple2<String, Iterable<String>>(line.split(",")[0], null));
		JavaPairRDD<String, Iterable<String>> UgenRDD = genRDD.distinct();
		JavaPairRDD<String, Iterable<String>> subRDD = UgenRDD.subtractByKey(senListRDD);	


		JavaPairRDD<String, Iterable<String>> finalRDD = subRDD.union(senListRDD);

		finalRDD.saveAsTextFile(outputPath);
		
		// Close the Spark context
		sc.close();
	}
}
