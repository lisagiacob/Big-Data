package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

//import javax.security.auth.SubjectDomainCombiner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath;
		//String outputPath;
		
		inputPath=args[0];
		//outputPath=args[1];

	
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

		/** Ex. 36: Average value
		 * Input: a collection of (structured) textual csv files containing the daily value of PM10 for a 
		 * set of sensors
		 * 	Each line of the files has the following format: sensorId,date,PM10 value (μg/m3 )\n
		 * Output: compute the average PM10 value
		 * 	print the result on the standard output
		 */

		JavaRDD<Double> PM10ValRDD = sensorRDD.map(line -> Double.parseDouble(line.split(",")[2]));
		
		SumCount zeroValue = new SumCount(0, 0);

		SumCount avg = PM10ValRDD.aggregate(zeroValue, (a,e) -> {
			a.sum = a.sum + e;
			a.numElements = a.numElements + 1;
			return a;
		}, 
		(a1, a2) -> {
			a1.sum = a1.sum + a2.sum;
			a1.numElements = a1.numElements + a2.numElements;
			return a1;
		});

		System.out.println(avg.avg());
		
		// Close the Spark context
		sc.close();
	}
}
