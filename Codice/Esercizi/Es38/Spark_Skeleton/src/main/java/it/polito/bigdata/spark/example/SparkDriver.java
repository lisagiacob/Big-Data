package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	
	public static void main(String[] args) throws IOException {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath;
		File outputPath;
		
		inputPath=args[0];
		outputPath=new File(args[1]);

	
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

		/** Ex. 38: Pollution analysis
		 * Input: a textual csv file containing the daily value of PM10 for a set of sensors
		 * 	Each line of the files has the following format: sensorId,date,PM10 value (μg/m3 )\n
		 * Output: the sensors with at least 2 readings with a PM10 value greater than the critical 
		 * 	threshold 50
		 * 	Store in an HDFS file the sensorIds of the selected sensors and also the number of times 
		 * 	each of those sensors is associated with a PM10 value greater than 50
		 */

		/*
		 * SOLUZIONE ALTERNATIVA! Potrei avere troppi sensori per fare la map!
		 * filter (PM! > 50)
		 * mapToPair((sensorId, 1))
		 * reduceByKey(sum())
		 * filter(p -> p._2 >= 2)
		 * saveAsTextFile()
		 */

		JavaPairRDD<String, Double> senPMRDD = sensorRDD.mapToPair(line -> new Tuple2<String, Double>(line.split(",")[0], Double.parseDouble(line.split(",")[2])));
		senPMRDD.filter(line -> {
			if (line._2() > 50) return true;
			else return false;
		});

		Map<String, Long> SensThre = senPMRDD.countByey();

		BufferedWriter bf = new BufferedWriter(new FileWriter(outputPath));
		
		for(Map.Entry<String, Long> e : SensThre.entrySet()){
			System.out.println(e.getKey() + ", " + e.getValue());
			bf.write(e.getKey() + ", " + e.getValue()); 
			// new line 
			bf.newLine(); 
		}
		bf.flush(); 
		bf.close();
		
		// Close the Spark context
		sc.close();
	}
}
