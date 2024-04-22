# Esempi Spark

### Exercise 44: Misleading profile selection

Input1:

A textual file containing the list of movies watched by the users of a video on demand service
userId,movieId,start-timestamp,end-timestamp

Input2: 

A textual file containing the list of prefences for each user
userId,movie-genre

Input3:

A textual file containing the list of movies with the associated information
movieid, title, movie-genre.

Output:

Select the userids of the lists of users with a misleading profile
A user has a misleading profile if more then threashold of the movies it has watch are not
associated with a movie genre it likes
Store the result in an HDFS file

```java
public class SparkDriver {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
	...
		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #44");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the watched movies file
		JavaRDD<String> watchedRDD = sc.textFile(inputPathWatched);

		// Select only the userid and the movieid
		// Define a JavaPairRDD with movieid as key and userid as value
		JavaPairRDD<String, String> movieUserPairRDD = watchedRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> movieUser = new Tuple2<String, String>(fields[1], fields[0]);

			return movieUser;
		});

		// Read the content of the movies file
		JavaRDD<String> moviesRDD = sc.textFile(inputPathMovies);

		// Select only the movieid and genre
		// Define a JavaPairRDD with movieid as key and genre as value
		JavaPairRDD<String, String> movieGenrePairRDD = moviesRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> movieGenre = new Tuple2<String, String>(fields[0], fields[2]);

			return movieGenre;
		});

		// Join watched movie with movies
		JavaPairRDD<String, Tuple2<String, String>> joinWatchedGenreRDD = movieUserPairRDD.join(movieGenrePairRDD);

		// Select only userid (as key) and genre (as value)
		JavaPairRDD<String, String> usersWatchedGenresRDD = joinWatchedGenreRDD
				.mapToPair((Tuple2<String, Tuple2<String, String>> userMovie) -> {
					// movieid - userid - genre
					Tuple2<String, String> movieGenre = new Tuple2<String, String>(userMovie._2()._1(),
							userMovie._2()._2());

					return movieGenre;
				});

		// Read the content of the preferences
		JavaRDD<String> preferencesRDD = sc.textFile(inputPathPreferences);

		// Define a JavaPairRDD with userid as key and genre as value
		JavaPairRDD<String, String> userLikedGenresRDD = preferencesRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> userGenre = new Tuple2<String, String>(fields[0], fields[1]);

			return userGenre;
		});

		// Cogroup the lists of watched and liked genres for each user
		// There is one pair for each userid
		// the value contains the list of genres (with repetitions) of the
		// watched movies and
		// the list of liked genres
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> userWatchedLikedGenres = usersWatchedGenresRDD
				.cogroup(userLikedGenresRDD);

		// Filter the users with a misleading profile
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> misleadingUsersListsRDD = userWatchedLikedGenres
				.filter((Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> listWatchedLikedGenres) -> {

					// Store in a local list the "small" set of liked genres
					// associated with the current user
					ArrayList<String> likedGenres = new ArrayList<String>();

					for (String likedGenre : listWatchedLikedGenres._2()._2()) {
						likedGenres.add(likedGenre);
					}

					// Count
					// - The number of watched movies for this user
					// - How many of watched movies are associated with a liked
					// genre
					int numWatchedMovies = 0;
					int notLiked = 0;

					for (String watchedGenre : listWatchedLikedGenres._2()._1()) {
						numWatchedMovies++;

						if (likedGenres.contains(watchedGenre) == false) {
							notLiked++;
						}
					}

					// Check if the number of watched movies associated with a non-liked genre
					// is greater that threshold%
					if ((double) notLiked > threshold * (double) numWatchedMovies) {
						return true;
					} else
						return false;
				});

		// Select only the userid of the users with a misleading profile
		JavaRDD<String> misleadingUsersRDD = misleadingUsersListsRDD.keys();

		misleadingUsersRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
```

```java
public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		String inputPathWatched;
		String inputPathPreferences;
		String inputPathMovies;
		String outputPath;
		double threshold;

		inputPathWatched = args[0];
		inputPathPreferences = args[1];
		inputPathMovies = args[2];
		outputPath = args[3];
		threshold = Double.parseDouble(args[4]);

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #44").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the watched movies file
		JavaRDD<String> watchedRDD = sc.textFile(inputPathWatched);

		// Select only the userid and the movieid
		// Define a JavaPairRDD with movieid as key and userid as value
		JavaPairRDD<String, String> movieUserPairRDD = watchedRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> movieUser = new Tuple2<String, String>(fields[1], fields[0]);

			return movieUser;
		});

		// Read the content of the movies file
		JavaRDD<String> moviesRDD = sc.textFile(inputPathMovies);

		// Select only the movieid and genre
		// Define a JavaPairRDD with movieid as key and genre as value
		JavaPairRDD<String, String> movieGenrePairRDD = moviesRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> movieGenre = new Tuple2<String, String>(fields[0], fields[2]);

			return movieGenre;
		});

		// Join watched movie with movies
		JavaPairRDD<String, Tuple2<String, String>> joinWatchedGenreRDD = movieUserPairRDD.join(movieGenrePairRDD);

		// Select only userid (as key) and movie genre (as value)
		JavaPairRDD<String, String> usersWatchedGenresRDD = joinWatchedGenreRDD
				.mapToPair((Tuple2<String, Tuple2<String, String>> userMovie) -> {
					// movieid - userid - genre
					Tuple2<String, String> movieGenre = new Tuple2<String, String>(userMovie._2()._1(),
							userMovie._2()._2());

					return movieGenre;
				});

		// Read the content of the preferences
		JavaRDD<String> preferencesRDD = sc.textFile(inputPathPreferences);

		// Define a JavaPairRDD with userid as key and genre as value
		JavaPairRDD<String, String> userLikedGenresRDD = preferencesRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> userGenre = new Tuple2<String, String>(fields[0], fields[1]);

			return userGenre;
		});

		// Count the number of watched movies for each user
		JavaPairRDD<String, Integer> usersNumVisualizationsRDD = usersWatchedGenresRDD.mapValues(moviegenre -> 1)
				.reduceByKey((v1, v2) -> v1 + v2);

		// Select the pairs (userid,movie-genre) that are not associated with a
		// movie-genre liked by userid
		JavaPairRDD<String, String> usersWatchedNotLikedRDD = usersWatchedGenresRDD.subtract(userLikedGenresRDD);

		// Count the number of watched movies for each user that are associated with a
		// not liked movie-genre
		JavaPairRDD<String, Integer> usersNumNotLikedVisualizationsRDD = usersWatchedNotLikedRDD
				.mapValues(moviegenre -> 1).reduceByKey((v1, v2) -> v1 + v2);

		// Join usersNumNotLikedVisualizationsRDD and usersNumVisualizationsRDD
		// and select only the users (userids) with a misleading profile with a filter
		JavaRDD<String> misleadingUsersRDD = usersNumNotLikedVisualizationsRDD.join(usersNumVisualizationsRDD)
				.filter(pair -> {
					int notLiked = pair._2()._1();
					int numWatchedMovies = pair._2()._2();

					// Check if the number of watched movies associated with a non-liked genre
					// is greater that threshold%
					if ((double) notLiked > threshold * (double) numWatchedMovies) {
						return true;
					} else
						return false;
				}).keys();

		misleadingUsersRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
```

### Exercise 45:

Input1:

A textual file containing the list of movies watched by the users of a video on demand service
userId,movieId,start-timestamp,end-timestamp

Input2: 

A textual file containing the list of prefences for each user
userId,movie-genre

Input3:

A textual file containing the list of movies with the associated information
movieid, title, movie-genre.

Output:

Select for each user with a misleading profile the list of movie genre s that are not in his/her preferred genres and are associated with at least 5 movies watched by the user.
A user has a misleading profile if more then threashold of the movies it has watch are not
associated with a movie genre it likes
Store the result in an HDFS file

```java
//AGGIUNGERE ALLA V1 DEL 44
// Select the pairs (userid,misleading genre)
		JavaPairRDD<String, String> misleadingUserGenrePairRDD = misleadingUsersListsRDD
				.flatMapValues((Tuple2<Iterable<String>, Iterable<String>> listWatchedLikedGenres) -> {

					ArrayList<String> selectedGenres = new ArrayList<String>();

					// Store the "small set" of liked genres in an ArrayList
					ArrayList<String> likedGenres = new ArrayList<String>();

					for (String likedGenre : listWatchedLikedGenres._2()) {
						likedGenres.add(likedGenre);
					}

					// In this solution I suppose that the number of distinct
					// genres is small and can be stored in a local Java variable.
					// The local Java variable is an HashMap that stores for
					// each non-liked genre also its number of occurrences in
					// the list of watched movies of the current user
					HashMap<String, Integer> numGenres = new HashMap<String, Integer>();

					// Select the watched genres that are not in the liked
					// genres and update their number of occurrences
					for (String watchedGenre : listWatchedLikedGenres._1()) {

						// Check if the genre is not in the liked ones
						if (likedGenres.contains(watchedGenre) == false) {
							// Update the number of times this genre appears
							// in the list of movies watched by the current user
							Integer num = numGenres.get(watchedGenre);
							
							if (num == null) {
								numGenres.put(watchedGenre, new Integer(1));
							} else {
								numGenres.put(watchedGenre, num + 1);
							}
						}
					}

					// Select the genres, which are not in the liked ones,
					// appearing at least 5 times
					for (String genre : numGenres.keySet()) {
						if (numGenres.get(genre) >= 5) {
							selectedGenres.add(genre);
						}
					}

					return selectedGenres;
				});

		misleadingUserGenrePairRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
```

### Exercise 46:

Input:

A textual file containing a set of temperature readings: timestamp, temperature
The sample rate is 1 minute

Output:

Consider all the windows containing 3 consecutive temperature readings and select the windows characterized by an increasing trend.

```java
public class SparkDriver {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		String inputPath;
		String outputPath;
		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #46");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the readings
		JavaRDD<String> readingsRDD = sc.textFile(inputPath);

		// Generate the elements of each window.
		// Each reading with start time t belongs to 3 windows
		// with a window size equal to 3:
		// - The one starting at time t-120s
		// - The one starting at time t-60s
		// - The one starting at time t
		JavaPairRDD<Integer, TimeStampTemperature> windowsElementsRDD = readingsRDD.flatMapToPair(reading -> {

			// ArrayList stores locally the three elements
			// (window start timestamp, current reading) associated with
			// the three windows containing this reading
			// TimeStampTemperature is a class that can be used to store a time
			// stamp and the associated
			// temperature value (i.e., TimeStampTemperature contains one
			// reading
			ArrayList<Tuple2<Integer, TimeStampTemperature>> pairs = new ArrayList<Tuple2<Integer, TimeStampTemperature>>();

			Tuple2<Integer, TimeStampTemperature> pair;

			String[] fields = reading.split(",");

			// fields[0] = time stamp
			// fields[1] = temperature

			// The current reading, associated with time stamp t,
			// is part of the windows starting at time t, t-60s, t-120s

			// Window starting at time t
			pair = new Tuple2<Integer, TimeStampTemperature>(Integer.parseInt(fields[0]),
					new TimeStampTemperature(Integer.parseInt(fields[0]), Double.parseDouble(fields[1])));
			pairs.add(pair);

			// Window starting at time t-60s
			pair = new Tuple2<Integer, TimeStampTemperature>(Integer.parseInt(fields[0]) - 60,
					new TimeStampTemperature(Integer.parseInt(fields[0]), Double.parseDouble(fields[1])));
			pairs.add(pair);

			// Window starting at time t-120s
			pair = new Tuple2<Integer, TimeStampTemperature>(Integer.parseInt(fields[0]) - 120,
					new TimeStampTemperature(Integer.parseInt(fields[0]), Double.parseDouble(fields[1])));
			pairs.add(pair);

			return pairs.iterator();
		});

		// Use groupbykey to generate one sequence for each time stamp
		JavaPairRDD<Integer, Iterable<TimeStampTemperature>> timestampsWindowsRDD = windowsElementsRDD.groupByKey();

		// Select the values. The key is useless for the next steps
		// Each value is one window composed on 3 elements (time stamp,
		// temperature)
		JavaRDD<Iterable<TimeStampTemperature>> windowsRDD = timestampsWindowsRDD.values();

		// Filter the windows based on their content
		JavaRDD<Iterable<TimeStampTemperature>> seletedWindowsRDD = windowsRDD
				.filter((Iterable<TimeStampTemperature> listElements) -> {

					// Store the 3 elements of the window in a HashMap
					// containing pairs (time stamp, temperature).
					HashMap<Integer, Double> timestampTemp = new HashMap<Integer, Double>();

					// Compute also the information about minimum time stamp
					int minTimestamp = Integer.MAX_VALUE;

					for (TimeStampTemperature element : listElements) {
						timestampTemp.put(element.getTimestamp(), element.getTemperature());

						if (element.getTimestamp() < minTimestamp) {
							minTimestamp = element.getTimestamp();
						}
					}

					// Check if the list contains three elements.
					// If the number of elements is not equal to 3 the window is
					// incomplete and must be discarded
					boolean increasing;

					if (timestampTemp.size() == 3) {
						// Check is the increasing trend is satisfied
						increasing = true;

						for (int ts = minTimestamp + 60; ts <= minTimestamp + 120 && increasing == true; ts = ts + 60) {

							// Check if temperature(t)>temperature(t-60s)
							if (timestampTemp.get(ts) <= timestampTemp.get(ts - 60)) {
								increasing = false;
							}
						}
					} else {
						increasing = false;
					}

					return increasing;
				});

		// seletedWindowsRDD
		seletedWindowsRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
```

```java
@SuppressWarnings("serial")
public class TimeStampTemperature implements Serializable {
	int timestamp; 
	double temperature;
	public TimeStampTemperature(int timestampValue, double temp) {
		this.timestamp=timestampValue;
		this.temperature=temp;
	}
	public void setTimestamp(int value) {
		this.timestamp=value;
	}
	public int getTimestamp() {
		return this.timestamp;
	}
	public void setTemperature(double value) {
		this.temperature=value;
	}
	public double getTemperature() {
		return this.temperature;
	}
	public String toString() {
		return new String(this.timestamp+","+this.temperature);
	}
}
```

### Exercise 43: Mapping Question-Answers

Input1: 

A large textual file containing a set of questions, each line contains one questions
QuestionId,Timestamp,TextOfTheQuestion

 Input2: 

A large textual file containing a set of answers, each line contains one answer
AnswerId,QuestionId,Timestamp,TextOfTheAnswer

Output: 

A file containing one line for each question
Each line contains a question and the list of answers to that question
QuestionId, TextOfTheQuestion, list of Answers

```java
public class SparkDriver {	
	public static void main(String[] args) {
		...		

		JavaRDD<String> questionsRDD = sc.textFile(inputPath1);
		JavaRDD<String> answersRDD = sc.textFile(inputPath2);
		
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
```

### Exercise 41: Top-k most critical sensors

Input: 

a textual csv file containing the daily value of PM10 for a set of sensors
sensorId, date, PM10 value (μg/m3)
The value of k

Output: 

an HDFS file containing the top-k critical sensors
The “criticality” of a sensor is given by the number of days with a PM10 value greater than 50.
Each line contains the number of critical days and the sensorId.
Consider only the sensor associated at least once with a value greater than 50

```java
public class SparkDriver {	
	public static void main(String[] args) {
		...

		JavaRDD<String> sensorRDD = sc.textFile(inputPath);

		JavaPairRDD<String, String> critSenRDD  = sensorRDD.filter(line -> {
			if(line.startsWith("sensorId")) return false;
			if(Double.parseDouble(line.split(",")[2]) < 50 ) return false;
			else return true;
		}).mapToPair(line -> new Tuple2<String, String>(line.split(",")[0], "1")).reduceByKey((val1, val2) -> {
			int val = Integer.parseInt(val1) + Integer.parseInt(val2);
			return val + "";
		}).mapToPair(line -> new Tuple2<String, String>(line._2(), line._1())).sortByKey();

		List<Tuple2<String, String>> topL = critSenRDD.take(2);
		JavaPairRDD<String, String> topRDD = sc.parallelizePairs(topL);
		topRDD.saveAsTextFile(outputPath);
		
		// Close the Spark context
		sc.close();
	}
}
```

### Exercise 39Bis: Critical dates analysis

Input: 

a textual csv file containing the daily value of PM10 for a set of sensors
sensorId, date, PM10 value (μg/m3)

Output: 

an HDFS file containing one line for each sensor, each line contains a sensorId and the list of dates with a PM10 value greater than 50 for that sensor
Also the sensors which have never been associated with a PM10 values greater than 50 must be included in the result (with an empty set)

```java
public class SparkDriver {	
	public static void main(String[] args) {
	...

		JavaRDD<String> sensorRDD = sc.textFile(inputPath);

		JavaRDD<String> filRDD = sensorRDD.filter(line -> Double.parseDouble(line.split(",")[2]) > 50);
		 
		JavaPairRDD<String, String> senDateRDD = filRDD.mapToPair(line -> 
								new Tuple2<String, String>(line.split(",")[0], line.split(",")[1]));
		
		JavaPairRDD<String, Iterable<String>> senListRDD = senDateRDD.groupByKey();

		JavaPairRDD<String, Iterable<String>> genRDD = sensorRDD.mapToPair(line -> 
								new Tuple2<String, Iterable<String>>(line.split(",")[0], null));

		JavaPairRDD<String, Iterable<String>> UgenRDD = genRDD.distinct();
		JavaPairRDD<String, Iterable<String>> subRDD = UgenRDD.subtractByKey(senListRDD);	

		JavaPairRDD<String, Iterable<String>> finalRDD = subRDD.union(senListRDD);

		finalRDD.saveAsTextFile(outputPath);
		
		// Close the Spark context
		sc.close();
	}
}
```