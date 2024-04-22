package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
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
		
		inputPath=args[0];
		outputPath=args[1];

	
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

		/** Ex. 35: Dates associated with the maximum value
		 * Input: a collection of (structured) textual csv files containing the daily value of PM10 for a 
		 * set of sensors
		 * 	Each line of the files has the following format: sensorId,date,PM10 value (μg/m3 )\n
		 * Output: the date(s) associated with the maximum value of PM10
		 */

		 String maxPM10 = sensorRDD.reduce((line1, line2) -> {
			Double PM1 = Double.parseDouble(line1.split(",")[2]);
			Double PM2 = Double.parseDouble(line2.split(",")[2]);
			if(PM1 > PM2) return line1;
			else return line2;
		});

		JavaRDD<String> maxPM10sRDD = sensorRDD.filter(line -> line.contains(maxPM10.split(",")[2]));

		JavaRDD<String> maxPM10DatesRDD = maxPM10sRDD.map(line -> line.split(",")[1]);

		maxPM10DatesRDD.saveAsTextFile(outputPath);
		
		// Close the Spark context
		sc.close();
	}
}
