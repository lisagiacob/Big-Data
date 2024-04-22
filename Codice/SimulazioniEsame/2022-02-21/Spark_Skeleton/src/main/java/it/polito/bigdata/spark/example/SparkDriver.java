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
		
		//Parte 1

		//SaleTimeStamp, UserName, ItemID, SalePrice
		JavaPairRDD<String, Integer> purchasesRDD = sc.textFile("Purchases.txt")
		//mantengo solo i purchases riferiti agli anni 2020 e 2021
		.filter(line -> {
			if(Integer.parseInt(line.split(",")[0].split("/")[0]) == 2020) return true;
			if(Integer.parseInt(line.split(",")[0].split("/")[0]) == 2021) return true;
			return false;
		})
		.mapToPair(line -> {
			//(ItemId,year, 1)
			return new Tuple2<String, Integer>(line.split(",")[2] + "," + line.split(",")[0].split("/")[0], 1);
		})
		//ottengo paris con key itemID,year e key il numero di acquisti di quell'anno
		.reduceByKey((v1,v2) -> v1+v2)
		.filter(line -> {
			if(line._2() < 10000) return false;
			return true;
		})
		//per ogni riga ottengo (item, 1)
		.mapToPair(line -> {
			String item = line._1().split(",")[0];
			return new Tuple2<String, Integer>(item , 1);
		})
		//Se un item è stato venduto almeno 10k volte entrmbi gli anni, dopo questa funzione 
		//dovrei avere (item, 2)
		.reduceByKey((v1,v2) -> v1+v2)
		.filter(line ->{
			if(line._2() == 2) return true;
			return false;
		});

		purchasesRDD.saveAsTextFile(outputPath);

		//Parte 2
		//Prendo gli item inclusi nel catalogo DAL 2020 <- DAL per poter poi usare subract by key
		JavaPairRDD<String, String> itemLateRDD = sc.textFile("ItemsCatalog.txt")
			.filter(line -> {
				if(Integer.parseInt(line.split(",")[3]) >= 2020) return true;
				return false;
			})
			//(Item, year)
			.mapToPair(line -> {
				return new Tuple2<String, String>(line.split(",")[0], line.split(",")[3]);
			});
		
		JavaPairRDD<String, Integer> purRDD = sc.textFile("Purchases.txt")
			.filter(line -> {
				if(Integer.parseInt(line.split(",")[0].split("/")[0]) == 2020) return true;
				return false;
			})
			// -> (itemID, month-user)
			.mapToPair(line ->{
				String month = line.split(",")[0].split("/")[1];
				return new Tuple2<String, String>(line.split(",")[2],month + "-" + line.split(",")[1]);
			})
			//Rimarranno solo gli item in catalogo PRIMA del 2020
			.subtractByKey(itemLateRDD)
			.map(line -> line._1()+ "," + line._2())
			//Tolgo tutte le volte che un item è stato compranto più da una volta nello stesso mese
			//dallo stesso cliente
			.distinct()
			//(ItemID,month, 1)
			.mapToPair(line -> new Tuple2<String, Integer>(line.split("-")[0], 1))
			.reduceByKey((v1,v2) -> v1+v2)
			.filter(line -> {
				if(line._2() < 10) return true;
				return false;
			});

		purRDD.saveAsTextFile(outputPath2);

		// Close the Spark context
		sc.close();
	}
}
