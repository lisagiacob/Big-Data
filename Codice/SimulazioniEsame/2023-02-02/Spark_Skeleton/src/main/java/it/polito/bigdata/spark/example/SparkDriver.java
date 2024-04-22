package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple1;
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
	
		//Houses that consumed a lot in 2022
		//(HouseID, a)
		JavaPairRDD<String, String> houseHighConsRDD = sc.textFile("DaolyPowerConsumption.txt").filter(line->{
			//i get the year and check its 2022, and the house has an high consumption
			if(line.split(",")[1].split("/")[0].equals("2022") && Integer.parseInt(line.split(",")[2]) > 30)
				return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, String>(line.split("")[0], "A"));

		//JavaPairRdd that associates all houses to their country
		//(HouseID, Country)
		JavaPairRDD<String, String> countriesRDD = sc.textFile("Houses.txt")
			.mapToPair(line -> new Tuple2<String, String>(line.split(",")[0], line.split(",")[2]));

		//RDD that associates all houses with an high consume (stored in houseHighCons RDD) woth their country
		//(HID, Country)
		//(Country) RDD with all the countries that have houses with high cons., without duplicates
		JavaRDD<String> CountryRDD = houseHighConsRDD.join(countriesRDD)
			.map(line -> line._2()._1()).distinct();
		
		//Rdd with all the Countries (no duplpicates)
		JavaRDD<String> allCountriesRDD = countriesRDD.map(line -> line._2()).distinct();

		//allCountries-CountriesWithHighEn = CountriesWithoutHighEn
		JavaRDD<String> noHighElRDD = allCountriesRDD.subtract(CountryRDD);

		noHighElRDD.saveAsTextFile(outputPath);

		//PARTE 2
		//Houses that cosumed a lot in 2021
		//(HID, 1)
		JavaPairRDD<String, Integer> houseHighCons2021RDD = sc.textFile("DailyPowerConsumption.txt").filter(line->{
			//i get the year and check its 2021, and the house has an high consumption
			if(line.split(",")[1].split("/")[0].equals("2021") && Integer.parseInt(line.split(",")[2]) > 30)
				return true;
			return false;
		}).mapToPair(line -> new Tuple2<String, Integer>(line.split(",")[0], 1));

		//JavaPairRdd that associates all houses to their city
		//(HouseID, city)
		JavaPairRDD<String, String> citiesRDD = sc.textFile("Houses.txt")
			.mapToPair(line -> new Tuple2<String, String>(line.split(",")[0], line.split(",")[1]));
	

		//RDD that associates all houses with an high consume (stored in houseHighCons2021RDD) with their country
		//(HID, (1, Country))
		//(Country, 1)
		//(country, n)
		//I keep only cities with more than 500 houses w/ high consues
		JavaPairRDD<String, Integer> CityHigh2021RDD = houseHighCons2021RDD.join(citiesRDD)
			.mapToPair(line -> {
				return new Tuple2<String, Integer>(line._2()._2(), line._2()._1());
			}).reduceByKey((v1,v2)->v1+v2)
			.filter(line ->{
				if(line._2() < 500) return false;
				return true;
			});

		CityHigh2021RDD.saveAsTextFile(outputPath2);
		// Close the Spark context
		sc.close();
	}
}
