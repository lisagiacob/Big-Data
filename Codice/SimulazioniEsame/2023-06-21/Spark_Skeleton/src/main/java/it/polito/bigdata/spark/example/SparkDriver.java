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


		String inputPath;
		String outputPath, outputPath2;
		
		inputPath=args[0];
		outputPath = args[1];
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
		
		// Read the content of the input file and keep only the business users
		JavaRDD <String> BusersRDD = sc.textFile("users.txt").filter(line -> {
			if(line.split(",").equals("Business")) return true;
			else return false;
		});
		// Non Business users
		JavaPairRDD <String, String> NusersRDD = sc.textFile("users.txt").filter(line -> {
			if(line.split(",").equals("Business")) return false;
			else return true;
		}).mapToPair(line -> {
			return new Tuple2<String, String>(line.split(",")[0],line.split(",")[4]);
		});

		// RDD of all the meetings done (UID, duration)
		// I then considerr only the meetings organized by a business user
		JavaPairRDD<String, String> UsDurRDD = sc.textFile("meetings.txt").mapToPair(line ->{
			return new Tuple2<String, String>(line.split(",")[4], line.split(",")[3]);
		}).subtract(NusersRDD);

		//RDD with the sum of the durations (UID, sum)
		JavaPairRDD<String, String> sumRDD = UsDurRDD.reduceByKey((v1,v2) -> {
			float sumDurations = Float.parseFloat(v1) + Float.parseFloat(v2);
			return sumDurations+"";
		});

		//RDD with the number of meeting organized by the same user (UID, num)
		JavaPairRDD<String, String> nMRDD = UsDurRDD.mapToPair(line ->{
			return new Tuple2<String, String>(line._1(), 1+"");
		}).reduceByKey((v1,v2) -> {
			int v = Integer.parseInt(v1) + Integer.parseInt(v2);
			return v+"";
		});

		//RDD with the max duration of the users meetings (UID, max)
		JavaPairRDD<String, String> maxRDD = UsDurRDD.reduceByKey((v1,v2) -> {
			if(Integer.parseInt(v1)>Integer.parseInt(v2)) return Integer.parseInt(v1)+"";
			else return Integer.parseInt(v2)+"";
		});

		//RDD with the min duration of the users meetings (UID, min)
		JavaPairRDD<String, String> minRDD = UsDurRDD.reduceByKey((v1,v2) -> {
			if(Integer.parseInt(v1)<Integer.parseInt(v2)) return Integer.parseInt(v1)+"";
			else return Integer.parseInt(v2)+"";
		});

		// statistcis RDD - (UID, (sumDurations,nMeetings))
		// statistcis RDD - (UID, avg)
		// statistcis RDD - (UID, ((avg,maxDur),minDur)) <- cogrup
		JavaPairRDD<String, Tuple2<Tuple2<String, String>, String>> statRDD = sumRDD.join(nMRDD).mapToPair(line->{
			float avg = Float.parseFloat(line._2()._1()) / Float.parseFloat(line._2()._2());
			return new Tuple2<String, String>(line._1(), avg+"");
		}).join(maxRDD).join(minRDD);

		statRDD.saveAsTextFile(outputPath);

		//PARTE 2
		// (UID, MID) - only issued by business users
		JavaPairRDD<String, String> usMeetRDD = sc.textFile("invitations.txt").mapToPair(line ->
			new Tuple2<String, String>(line.split(",")[1], line.split(",")[0])).subtract(NusersRDD);
			
		// (MID, 1)
		// (MID, numPart)
		JavaPairRDD<String, Integer> MeetParRDD = usMeetRDD.mapToPair(line -> 
			new Tuple2<String, Integer>(line._2(), 1)).reduceByKey((v1,v2) -> v1+v2);

		JavaPairRDD<String, String> smallMeetRDD = MeetParRDD.filter(line -> {
			if(line._2() < 5) return true;
			else return false;
		}).mapToPair(line -> new Tuple2<String, String>(line._1(), line._2()+""));

		JavaPairRDD<String, Integer> bigMeetRDD = MeetParRDD.filter(line -> {
			if(line._2() > 20) return true;
			else return false;
		});

		JavaPairRDD<String, Integer> medMeetRDD = MeetParRDD.filter(line -> {
			if(line._2() > 5 && line._2()<=20) return true;
			else return false;
		});

		// Java Pair RDD with all meeting with invitations sent (MID, UID)
		JavaPairRDD<String, String> meetUsRDD = usMeetRDD.mapToPair(line -> 
			new Tuple2<String, String>(line._2(), line._1()));
		
		// meetings with 0 invitations - (MID, dur)
		JavaPairRDD<String, String> microMRDD = UsDurRDD.subtract(meetUsRDD);
		
		// small and micro meeting - (MID, n)
		JavaPairRDD<String, String> allSmallRDD = smallMeetRDD.union(microMRDD);


		// (MID, (d, UID))
		// (UID, 1) - RDD di tutti gli utenti e i loro meeting piccoli
		// (UID, numSmallMeetings)
		JavaPairRDD<String, Integer> numSmallRDD = allSmallRDD.join(meetUsRDD).mapToPair(line ->
			new Tuple2<String, Integer>(line._2()._2(), 1))
			.reduceByKey((v1, v2)-> v1+v2);

		JavaPairRDD<String, Integer> numMedRDD = medMeetRDD.join(meetUsRDD).mapToPair(line ->
			new Tuple2<String, Integer>(line._2()._2(), 1))
			.reduceByKey((v1, v2)-> v1+v2);

		JavaPairRDD<String, Integer> numBigRDD = bigMeetRDD.join(meetUsRDD).mapToPair(line ->
			new Tuple2<String, Integer>(line._2()._2(), 1))
			.reduceByKey((v1, v2)-> v1+v2);
		
		//(UID, ((nSmall, nMedium), nBig))
		JavaPairRDD<String, Tuple2<Tuple2<Integer, Integer>, Integer>> distRDD = numSmallRDD
			.join(numMedRDD).join(numBigRDD);

		distRDD.saveAsTextFile(outputPath2);
		// Close the Spark context
		sc.close();
	}
}
