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
		JavaRDD<String> logReqRDD = sc.textFile(inputPath);

		/** Ex. 30: 
		 * Input: a simplified log of a web server - each line is associated with a URL request
		 * Output: the list of distinct IP addresses associated with the connections to a google page 
		 * (i.e., connections to URLs containing the term “www.google.com”)
		 */
		JavaRDD<String> googleReqRdd = logReqRDD.filter(line -> line.contains("www.google.com"));
		JavaRDD<String> IpAddRDD = googleReqRdd.map(line -> line.split("\\s+")[0]);

		IpAddRDD.saveAsTextFile(outputPath);
		

		// Close the Spark context
		sc.close();
	}
}
