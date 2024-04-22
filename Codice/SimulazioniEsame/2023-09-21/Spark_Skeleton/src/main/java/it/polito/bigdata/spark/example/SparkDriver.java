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
		
		// Read the content of the input file and keep only the infos about the comedy TV series
		JavaPairRDD<String, Integer> comedyRDD = sc.textFile("TVSeries.txt").filter(line -> {
			if(line.contains("Comedy")) return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, Integer>(line.split(",")[0], 0));

		//Ottengo un RDD fatto di pairs (Sid, NStagione)
		JavaPairRDD<String, Integer> TepRDD = sc.textFile("episodes.txt").mapToPair(line ->  
				new Tuple2<String, Integer>(line.split(",")[0], Integer.parseInt(line.split(",")[1])));
		
		
		//ComNum = Tep - comedyRDD = 102
		JavaPairRDD<String, Integer> ComNumRDD = TepRDD.subtractByKey(comedyRDD);

		//epRDD = Tep - ComNum
		//Ottengo un RDD fatto di pairs (Sid, NStagione) con Sid univoco
		JavaPairRDD<String, Integer> epRDD = TepRDD.subtractByKey(ComNumRDD);

		//Ottengo un RDD con solo pairs (SID, nStag) diversi tra loro
		JavaPairRDD<String, Integer> prova = epRDD.distinct();
		//Ottengo una mappa con chiave SID e valore il numero di stagioni di quella serie
		Map<String, Long> nStag = prova.countByKey();

		//RDD con (SID, numEp), non mi serve sapere quanti episodi per ogni serie, perchè comunque dovrei sommarli dopo
		JavaPairRDD<String, Integer> SidNepRDD = epRDD.mapToPair(line -> new Tuple2<String, Integer>(line._1(), 1))
			.reduceByKey((ep1, ep2) -> {
				return ep1 + ep2;
			});
		
		JavaPairRDD<String, Long> avgRDD = SidNepRDD.mapToPair(line-> {
			return new Tuple2<String, Long> (line._1(),  line._2()/nStag.get(line._1()));
		});

		//RISULTATO 1
		avgRDD.saveAsTextFile(outputPath);

		//Parte 2
		// Ottengo inizialmente un PairRDD ((CID,SID,Snum), 1), poi faccio distinct in modo che indipendentemente
		// da quanti episodi di una serie sono stati visti, comparga una sola visualizzazione per stagione, poi
		// creo un JavaPairRDD ((CID,SID), num) (passando da un pair ((CID,SID), 1)) dove num è il numero di stagioni di xui è stato visto almeno un ep.
		// a questo punto se num è uguale al numero di stagioni della serie (calcolato per il p1, presente nella map nStag)
		// allora ok, tengo e ottengo un RDD (SID, CID).
		JavaPairRDD<String, String> CliSidRDD = sc.textFile("CustomerWatched.txt").mapToPair(line -> {
			return new Tuple2<String, Integer>(line.split(",")[0] + "," + line.split(",")[2] + "," + line.split(",")[3],1);
		}).distinct().mapToPair(line->{
			return new Tuple2<String, Integer>(line._1().split(",")[0] + "," + line._1().split(",")[1], 1);
		}).reduceByKey((v1,v2) -> v1+v2).mapToPair(line -> {
			if((long)line._2() == nStag.get(line._1().split(",")[1])) return line;
			return new Tuple2<String,Integer>(null, 0);
		}).filter(line -> {
			if (line._1()==null) return false;
			return true;
		}).mapToPair(line->{
			return new Tuple2<String, String>(line._1().split(",")[1], line._1().split(",")[0]);
		});

		CliSidRDD.saveAsTextFile(outputPath2);


		// Close the Spark context
		sc.close();
	}
}
