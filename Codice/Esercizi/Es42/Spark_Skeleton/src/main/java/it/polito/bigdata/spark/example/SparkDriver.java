package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath1, inputPath2;
		String outputPath;
		
		inputPath1 = "ex42_data_questions";
		inputPath2 = "ex42_data_answers";
		outputPath = "out";

	
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
		JavaRDD<String> questionsRDD = sc.textFile(inputPath1);
		JavaRDD<String> answersRDD = sc.textFile(inputPath2);

		/** Ex. 42: Mapping question-answers
		 * Input1: A large textual file containing a set of questions
		 * 	Each line contains one questions
		 * 	Each line has the following format
		 * 	QuestionId,Timestamp,TextOfTheQuestion
		 * Input2: A large textual file containing a set of answers
		 * 	Each line contains one answer
		 * 	Each line has the format
		 * 	AnswerId,QuestionId,Timestamp,TextOfTheAnswer
		 * Output:
		 *	A file containing one line for each question
		 *	Each line contains a question and the list of answers to that question
		 *	QuestionId, TextOfTheQuestion, list of Answers
		*/
		
		JavaPairRDD<String, String> qPairRDD = questionsRDD.mapToPair(line -> {
			return new Tuple2<String, String>(line.split(",")[0], line.split(",")[2]);
		});

				
		JavaPairRDD<String, String> aPairRDD = answersRDD.mapToPair(line -> {
			return new Tuple2<String, String>(line.split(",")[1], line.split(",")[3]);
		});
		
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> qaPairRDD = qPairRDD.cogroup(aPairRDD);
		
		qaPairRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
