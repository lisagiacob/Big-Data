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

// Settembre 21 2023
public class SparkDriver {
	
	public static void main(String[] args) throws IOException {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String outputPath, outputPath2;
		
		outputPath = "out";
		outputPath2 = "out2";

	
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
		
		JavaRDD<String> DataCenterRDD = sc.textFile("DataCenters.txt");
		long nDC = DataCenterRDD.count();

		//PairRDD with as key the dates and as value the number of times the pm > 1000 in a center
		//if in a date >= 90 percent did, i save the date in the file.
		JavaPairRDD<String, Integer> DatesRDD = sc.textFile("DailyPowerConsumption")
			.filter(line -> {
				Float pm = Float.parseFloat(line.split(",")[2]);
				if(pm > 1000) return true;
				return false;
			}).mapToPair(line -> new Tuple2<String, Integer>(line.split(",")[1], 1))
			.reduceByKey((v1, v2) -> v1+v2)
			.filter(line -> {
				if((line._2() * nDC)/100 >= 90) return true;
				return false;
			});

		DataCenterRDD.saveAsTextFile(outputPath);

		//PARTE 2
		


		// Close the Spark context
		sc.close();
	}
}
