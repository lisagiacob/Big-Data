package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
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
		String prefix;
		
		inputPath=args[0];
		outputPath=args[1];
		prefix=args[2];

	
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
		JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);

		// Task 1
		// Only the elements of the RDD satisfying the filter imposed by meansare included in the
		// googleRDD RDD
		JavaRDD<String> prefixRDD = wordFreqRDD.filter(logLine -> logLine.toLowerCase().contains(prefix));

		// Store the result in the output folder
		prefixRDD.saveAsTextFile(outputPath);

		// Number of selected lines
		long nSelected = prefixRDD.count();
		System.out.println("Number of selected lines: " + nSelected);

		// Frequency
		JavaRDD<Integer> freq = prefixRDD.map(word -> word.split("\\s+")[1]).map(Integer::parseInt);

		List<Integer> maxFreq = freq.top(1);
		System.out.println("Max freq:" + maxFreq.get(0));
		
		// Task 2
		//JavaRDD<Integer> freq2 = freq.filter(f -> f.intValue() > (80*maxFreq.get(0))/100);
		JavaRDD<String> task2 = wordFreqRDD.filter(
			line -> {
				int count = Integer.parseInt(line.split("\\s+")[1]);
				return count > 80*maxFreq.get(0)/100;
			});
		
		long nFreq = task2.count();
		System.out.println("Number of lines with an high frequency: " + nFreq);

		JavaRDD<String> wordRDD = task2.map(line -> line.split("\\+s")[0]);
		wordRDD.saveAsTextFile("OutFile");
		

		// Close the Spark context
		sc.close();
	}
}
