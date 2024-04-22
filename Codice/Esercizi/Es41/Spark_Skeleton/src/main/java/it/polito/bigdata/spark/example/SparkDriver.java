package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

import java.util.List;

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

		/** Ex. 41: Top-k most critical sensors
		 * Input: a textual csv file containing the daily value of PM10 for a set of sensors
		 * 	Each line of the files has the following format: sensorId, date, PM10 value (μg/m3)
		 * 	The value of k
		 * Output: an HDFS file containing the top-k critical sensors
		 * 	The “criticality” of a sensor is given by the number of days with a PM10 value greater 
		 * 	than 50. Each line contains the number of critical days and the sensorId.
		 * 	Consider only the sensor associated at least once with a value greater than 50
		 */
		JavaPairRDD<String, String> critSenRDD  = sensorRDD.filter(line -> {
			if(line.startsWith("sensorId")) return false;
			if(Double.parseDouble(line.split(",")[2]) < 50 ) return false;
			else return true;
		}).mapToPair(line -> new Tuple2<String, String>(line.split(",")[0], "1")).reduceByKey((val1, val2) -> {
			int val = Integer.parseInt(val1) + Integer.parseInt(val2);
			return val + "";
		}).mapToPair(line -> new Tuple2<String, String>(line._2(), line._1())).sortByKey();



		List<Tuple2<String, String>> topL = critSenRDD.take(2);
		JavaPairRDD<String, String> topRDD = sc.parallelizePairs(topL);
		topRDD.saveAsTextFile(outputPath);
		
		// Close the Spark context
		sc.close();
	}
}
