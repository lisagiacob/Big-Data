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


		String inputPath1, inputPath2, inputPath3;
		String outputPath;
		int threshold;
		
		inputPath3 = "ex44_data/movies.txt";
		inputPath2 = "ex44_data/preferences.txt";
		inputPath1 = "ex44_data/watchedmovies.txt";
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
		
		/** Ex. 44: Misleading profile selection
		 * Input1: a textual file containing the list of movies watched by the users of a videno on demand 
		 * 	service, each line contains the information about one of the visualization
		 * 	useid, movieid, start-timestamp, end-timestamp
		 * Input2: A textual file containing the list of preferences for each user
		 * 	Eache line contains the information about one preference
		 * 	userid, movie-genre
		 * Input3: a textual file containing the list of movies with the associated information
		 * 	Each line contains the information about one movie
		 * 	movieid, title, movie-genre
		 * Output: Select the userids of the lists of users with a misleading profile
		 * 	A user has a misleading profile if more then theashold of the movies it has watch are not 
		 * 	associated with a movie genre it likes
		 * Store the results in an HDFS file
		*/

		// Read the content of the input file/folder
		// userRDD avrà movieid come chiave e userid come value
		JavaPairRDD<String, String> userRDD = sc.textFile(inputPath1).mapToPair(line-> {
			Tuple2<String, String> userMovie = new Tuple2<String,String>(line.split("\\,")[1], line.split("\\,")[0]);
			return userMovie;
		});
		JavaPairRDD<String, String> prefRDD = sc.textFile(inputPath2).mapToPair(line-> {
			Tuple2<String, String> userGen = new Tuple2<String,String>(line.split(",")[0], line.split(",")[1]);
			return userGen;
		});
		// movieRDD ha movieid come chiave e genre come value
		JavaPairRDD<String, String> moviesRDD = sc.textFile(inputPath3).mapToPair(line-> {
			Tuple2<String, String> userMovie = new Tuple2<String,String>(line.split("\\,")[0], line.split("\\,")[2]);
			return userMovie;
		});
		
		// unisco i due pair rdd sulla loro chiave (movie), ottenendo così: (movie, (user, genre)) e poi li mappo in modo 
		// da avere un pairRDD con chiaver user e key genre
		JavaPairRDD<String, Tuple2<String, String>> usermovieRDD = userRDD.join(moviesRDD);
		
		JavaPairRDD<String, String> userGenRDD = usermovieRDD.mapToPair(line ->{
			Tuple2<String, String> userGen = new Tuple2<String,String>(line._2()._1(), line._2()._2());
			return userGen;
		});

		JavaPairRDD<String, Tuple2<String, String>> userPrefRDD = userGenRDD.join(prefRDD);
				
		userGenRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
