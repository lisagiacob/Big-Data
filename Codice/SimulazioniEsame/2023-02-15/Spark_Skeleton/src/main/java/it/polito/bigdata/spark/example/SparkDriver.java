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
		
		//(HID,(month, m3))
		JavaPairRDD<String, String> houseRDD = sc.textFile("MonthlyWaterConsumption.txt").mapToPair(line->{
			return new Tuple2<String, String>(line.split(",")[0],line.split(",")[1]+","+line.split(",")[2]);
		});

		//RDDs of the 4 different trimesters.
		//I change the fomat to (HID, M3)
		//Sum the M3 values of the 3 months in the same hpuses -> one line per house
		JavaPairRDD<String, String> pT22RDD = houseRDD.filter(line -> {
			if(line._2().split(",")[0].equals("2022/1") || line._2().split(",")[0].equals("2022/2") || line._2().split(",")[0].equals("2022/3"))
				return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, String>(line._1(), line._2().split(",")[1]))
			.reduceByKey((v1, v2) -> v1+v2);

		JavaPairRDD<String, String> sT22RDD = houseRDD.filter(line -> {
			if(line._2().split(",")[0].equals("2022/4") || line._2().split(",")[0].equals("2022/4") || line._2().split(",")[0].equals("2022/6"))
				return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, String>(line._1(), line._2().split(",")[1]))
			.reduceByKey((v1, v2) -> v1+v2);

		JavaPairRDD<String, String> tT22RDD = houseRDD.filter(line -> {
			if(line._2().split(",")[0].equals("2022/7") || line._2().split(",")[0].equals("2022/8") || line._2().split(",")[0].equals("2022/9"))
				return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, String>(line._1(), line._2().split(",")[1]))
			.reduceByKey((v1, v2) -> v1+v2);

		JavaPairRDD<String, String> qT22RDD = houseRDD.filter(line -> {
			if(line._2().split(",")[0].equals("2022/10") || line._2().split(",")[0].equals("2022/11") || line._2().split(",")[0].equals("2022/12"))
				return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, String>(line._1(), line._2().split(",")[1]))
			.reduceByKey((v1, v2) -> v1+v2);

		//SAME AS BEFORE, BUT FOR 2021
		//RDDs of the 4 different trimesters.
		//I change the fomat to (HID, M3)
		//Sum the M3 values of the 3 months in the same hpuses -> one line per house
		JavaPairRDD<String, String> pT21RDD = houseRDD.filter(line -> {
			if(line._2().split(",")[0].equals("2021/1") || line._2().split(",")[0].equals("2021/2") || line._2().split(",")[0].equals("2021/3"))
				return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, String>(line._1(), line._2().split(",")[1]))
			.reduceByKey((v1, v2) -> v1+v2);

		JavaPairRDD<String, String> sT21RDD = houseRDD.filter(line -> {
			if(line._2().split(",")[0].equals("2021/4") || line._2().split(",")[0].equals("2021/4") || line._2().split(",")[0].equals("2021/6"))
				return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, String>(line._1(), line._2().split(",")[1]))
			.reduceByKey((v1, v2) -> v1+v2);

		JavaPairRDD<String, String> tT21RDD = houseRDD.filter(line -> {
			if(line._2().split(",")[0].equals("2021/7") || line._2().split(",")[0].equals("2021/8") || line._2().split(",")[0].equals("2021/9"))
				return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, String>(line._1(), line._2().split(",")[1]))
			.reduceByKey((v1, v2) -> v1+v2);

		JavaPairRDD<String, String> qT21RDD = houseRDD.filter(line -> {
			if(line._2().split(",")[0].equals("2021/10") || line._2().split(",")[0].equals("2021/11") || line._2().split(",")[0].equals("2021/12"))
				return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, String>(line._1(), line._2().split(",")[1]))
			.reduceByKey((v1, v2) -> v1+v2);

		//I join the RDDs of the two years (for each trimester)
		//(HID, (M322, M321))
		//I setthe houses where there is an increas in consumption woth a 1 value and the other 0 value
		//(HID, 1) || (HID, 0)
		JavaPairRDD<String, Integer> incrPRDD = pT22RDD.join(pT21RDD).mapToPair(line ->{
			if(Integer.parseInt(line._2()._1()) < Integer.parseInt(line._2()._2())) 
				return new Tuple2<String,Integer>(line._1(), 1);
			return new Tuple2<String,Integer>(line._1(), 0);
		});
		JavaPairRDD<String, Integer> incrSRDD = sT22RDD.join(sT21RDD).mapToPair(line ->{
			if(Integer.parseInt(line._2()._1()) < Integer.parseInt(line._2()._2())) 
				return new Tuple2<String,Integer>(line._1(), 1);
			return new Tuple2<String,Integer>(line._1(), 0);
		});
		JavaPairRDD<String, Integer> incrTRDD = tT22RDD.join(tT21RDD).mapToPair(line ->{
			if(Integer.parseInt(line._2()._1()) < Integer.parseInt(line._2()._2())) 
				return new Tuple2<String,Integer>(line._1(), 1);
			return new Tuple2<String,Integer>(line._1(), 0);
		});
		JavaPairRDD<String, Integer> incrQRDD = qT22RDD.join(qT21RDD).mapToPair(line ->{
			if(Integer.parseInt(line._2()._1()) < Integer.parseInt(line._2()._2())) 
				return new Tuple2<String,Integer>(line._1(), 1);
			return new Tuple2<String,Integer>(line._1(), 0);
		});

		//I join the 4 RDDs, and if the resulting line of each house hasn't at least 3 1 values, I discard it
		//(HID, (((P,S),T),Q))
		//(HID)
		JavaPairRDD<String, String> incrRDD = incrPRDD.join(incrSRDD).join(incrTRDD).join(incrQRDD)
			.filter(line -> {
				if(line._2()._1()._1()._1() + line._2()._1()._1()._2()+ line._2()._1()._2() + line._2()._2() == 3) return true;
				return false;
			}).mapToPair(line -> new Tuple2<String, String>(line._1(), 1+""));

		//I use two subtract to obtain an RDD made of the sole houses that saw an encrease in consumption
		JavaPairRDD<String,String> housesNRDD = sc.textFile("Houses.txt").mapToPair(line -> new Tuple2<String, String>(line.split(",")[0], line.split(",")[1]))
			.subtract(incrRDD);
		JavaPairRDD<String, String> housesRDD = sc.textFile("Houses.txt").mapToPair(line -> new Tuple2<String, String>(line.split(",")[0], line.split(",")[1]))
			.subtract(housesNRDD);

		//RISULTATO 1
		housesRDD.saveAsTextFile(outputPath);

		//PARTE 2
		
		JavaPairRDD<String, String> annual22RDD = houseRDD.filter(line -> {
			if(line._2().split(",")[0].split("/")[0].equals("2022"))
				return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, String>(line._1(), line._2().split(",")[1]))
			.reduceByKey((v1, v2) -> Integer.parseInt(v1)+Integer.parseInt(v2)+"");
		JavaPairRDD<String, String> annual21RDD = houseRDD.filter(line -> {
			if(line._2().split(",")[0].split("/")[0].equals("2021"))
				return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, String>(line._1(), line._2().split(",")[1]))
			.reduceByKey((v1, v2) -> Integer.parseInt(v1)+Integer.parseInt(v2)+"");

		JavaRDD<String> decrRDD = annual22RDD.join(annual21RDD).filter(line ->{
			if(Integer.parseInt(line._2()._1()) < Integer.parseInt(line._2()._2())) return true;
			return false;
		}).map(line -> line._1());

		//I use two subtract to obtain an RDD made of the sole houses that saw an encrease in consumption
		JavaPairRDD<String,String> cityNRDD = sc.textFile("houses").mapToPair(line -> new Tuple2<String, String>(line.split(",")[0], line.split(",")[1]))
			.subtract(incrRDD);
		JavaRDD<String> citiesRDD = sc.textFile("houses").mapToPair(line -> new Tuple2<String, String>(line.split(",")[0], line.split(",")[1]))
			.subtract(housesNRDD).map(line -> line._2()).distinct();

		citiesRDD.saveAsTextFile(outputPath2);

		// Close the Spark context
		sc.close();
	}
}
