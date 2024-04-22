package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
		
		inputPath="ReviewsSample.csv";
		outputPath="out";

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #6")
			.setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #6").setMaster("local");
		// Remember to remove .setMaster("local") before running your application on the cluster
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: "+JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");
		
		// Read the content of the input file
		JavaRDD<String> inputRDDI = sc.textFile(inputPath);

		// TODO .......
		JavaRDD<String> inputRDD = inputRDDI.filter(line -> {
			if(!line.startsWith("Id")) return true;
			else return false;
		});
		JavaPairRDD<String, String> userProductRDD = inputRDD.mapToPair(line -> new Tuple2<String, String>(line.split(",")[2], line.split(",")[1]));
		userProductRDD.distinct();
		JavaPairRDD<String, Iterable<String>> UPsRDD = userProductRDD.groupByKey();
//		UPsRDD.saveAsTextFile(outputPath);

		//Punto 2
		JavaPairRDD<String, Integer> ArticlePairsRDD = UPsRDD.mapToPair(line -> {
			String[] arts = new String[200];
			int i = 0;
			for(String s: line._2()){
				arts[i] = s;
				i++;
			}
			for(int j=0; j<i; j++){
				String artPair = arts[0] + "," + arts[i];
				return new Tuple2 <String, Integer> (artPair, 1);
			}
			return null;
		});
		ArticlePairsRDD.saveAsTextFile(outputPath);
		
		// Store the result in the output folder
		//resultRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}