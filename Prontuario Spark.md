# Prontuario Spark

## Transformations:

JavaRDD<T> filter(Function<T, Boolean> predicate); 
returns a new RDD containing only the elements that satisfy the *predicate* (← user specified condition)

```java
JavaRDD<String> errorsRDD = inputRDD.filter(x -> x.contains("error"));
JavaRDD<String> critSenRDD = sensorRDD.filter(line -> {
			if(Integer.parseInt(line.split(",")[2]) < 50 ) return false; //element discarded
			else return true; //element selected
		});
```

JavaRDD<R> map(Function<T,R> mapper); 
returns a new RDD applying the *mapper* function on each element of the input RDD.
The new RDD contains exactly one element y for each element x of the input RDD. The data types of x and y can be different.

```java
JavaRDD<String> IpAddRDD = googleReqRdd.map(line -> line.split("\\s+")[0]);
```

JavaRDD<R> flatMap(FlatMApFunctino<T, R>)
returns a new RDD by applying a function f on each element of the input RDD. The new RDD contains a list of elements obtained by applying f on each element x of the input RDD ← [y] = f(x), where [y] can be empty. x and y can have different data types.
The final result is the concatenation of the list of values obtained by applying f over all the elementsin the input RDD - duplicates are not removed!

```java
JavaRDD<String> inputRDD = sc.textFile("document.txt");
//Compute the list of words occurring in tocument.txt
JavaRDD<String> listOfWordsRDD = inputRDD.flatMap(x -> Arrays.asList(x.split("")).iterator();
//The new RDD will contain the Strings made by the concatenation of the lists obtained
```

JavaRDD<T> distinct()
The distinct transformation is applied on one single RDD and returns a new RDD containing the list of distinct elements of the input RDD.
Shuffle executed.

```java
JavaRDD<String> inputRDD = sc.textFile("document.txt");
JavaRDD<String> distinctNamesRDD = inputRDD.distinct();
```

JavaRDD<T> sample(boolean withReplacement, double fraction)
The sample transformation is applied on one single RDD and returns a new RDD containing a random sample of the elements of the input RDD.
withReplacement specifies if the random sample is with replacement (true) or not (false).
fraction specifies the expected size of the sample as a fraction of the input RDD’s size [0,1]

```java
JavaRDD<String> randomSentencesRDD = inputRDD.sample(false,0.2)
```

JavaRDD<T> union(JavaRDD<T> secondInputRDD)
The result is the union of the two input RDDs. Duplicates elements are not removed

```java
JavaPairRDD<String, Iterable<String>> finalRDD = subRDD.union(senListRDD);
```

### **JavaPair:**

JavaPairRDD<K,V> mapToPair(PairFunction<T,K,V> function)
creates a new PairRDD by applying a function on each element of the “regular” input RDD

```java
JavaPairRDD<String, Integer> nameOneRDD = namesRDD.mapToPair(name -> new Tuple2<String, Integer>(name, 1));
```

JavaPairRDD<K,V> flatMapToPair(PairFlatMapFunction<T,K,V> function)
creates a new PairRDD by applying a function f on each element of the “input” RDD. The new PairRDD contains a list of pairs obtained by
applying f on each element x of the “input” RDD.

```java
JavaPairRDD<String, Integer> wordOneRDD = linesRDD.flatMapToPair(line -> {
	List<Tuple2<String, Integer>> pairs = new ArrayList<Tuple2<String, Integer>>();
	String[] words = line.split(" ");
	for (String word : words) {
		pairs.add(new Tuple2<String, Integer>(word, 1));
	}
	return pairs.iterator();
});
```

JavaPairRDD<K,V> parallelizePairs(java.util.List<scala.Tuple2<K,V>> list)
used to create a new PairRDD from a local Java in-memory collection

```java
ArrayList<Tuple2<String, Integer>> nameAge = new ArrayList<Tuple2<String, Integer>>();
...
JavaPairRDD<String, Integer> nameAgeRDD = sc.parallelizePairs(nameAge);
```

JavaPairRDD<K,V> reduceByKey(Function2<V,V,V> f)
Create a new PairRDD where there is one pair for each distinct key k of the input PairRDD. The value associated with key k in the new PairRDD is computed by applying a user-provided function (associative and commutative!) on the values associated with k in the input PairRDD.
The data type of the new PairRDD must be the same of the input PairRDD.
Shuffle executed.
!!! l’output non deve solo essere dello stesso tipo, ma ANCHE avere lo stesso formato dell’input!

```java
JavaPairRDD<String, Integer> youngestPairRDD = nameAgeRDD.reduceByKey( (age1, age2) -> {max()});
```

JavaPairRDD<K, V> foldByKey(V zeroValue, Function2<V, V, V> f)
Create a new PairRDD where there is one pair for each distinct key k of the input PairRDD. The value associated with key k in the new PairRDD is computed by applying a user-provided function (associative!) on the values associated with k in the input PairRDD.
Shuffle executed.
!!! l’output non deve solo essere dello stesso tipo, ma ANCHE avere lo stesso formato dell’input!

JavaPairRDD<K, U> combineByKey(Function<V, U> createCombiner, Function2<U, V, U> mergeValue, Function2<U, U, U> mergeCombiner)
The type for the input PairRDD is V, the type for the returned PairRDD is U and the type for both the keys is K.
The data typer of the values of the input and the returned RDD of pairs can be different.
Shuffle executed.

```java
// This class is used to store a total sum of values and the number of
// summed values. It is used to compute the average 
public class AvgCount implements Serializable {
	public int total;
	public int numValues;
	public AvgCount(int tot, int num) {
		total=tot;
		numValues=num;
	}
	public double average() {
		return (double)total/(double)numValues;
	}
	public String toString() {
		return new String(""+this.average());
	}
}
...
// Create the JavaPairRDD from the local collection
JavaPairRDD<String, Integer> nameAgeRDD = sc.parallelizePairs(nameAge);

JavaPairRDD<String, AvgCount> avgAgePerNamePairRDD=nameAgeRDD.combineByKey( inputElement -> 
	new AvgCount(inputElement, 1),
	(intermediateElement, inputElement) -> { AvgCount combine = new AvgCount(inputElement, 1);
		combine.total=combine.total+intermediateElement.total;
		combine.numValues = combine.numValues+
		intermediateElement.numValues;
		return combine;
	},
	(intermediateElement1, intermediateElement2) -> {
		AvgCount combine = new AvgCount(intermediateElement1.total,
		intermediateElement1.numValues);
		combine.total=combine.total+intermediateElement2.total;
		combine.numValues=combine.numValues+
		intermediateElement2.numValues;
		return combine;
	}
);
avgAgePerNamePairRDD.saveAsTextFile(outputPath);
```

JavaPairRDD<K, Iterable<V>> groupByKey()
Create a new PairRDD where there is one pair foreach distinct key k of the input PairRDD. The value associated with key k in the new PairRDD is the list of values associated with k in the input PairRDD ← useful if you need to apply an aggregation/compute a function that is not associative
Shuffle executed. ← try to use reduceByKey(), aggregateByKey() or combineByKey() if possible.

```java
JavaPairRDD<String, Iterable<Integer>> agesPerNamePairRDD = nameAgeRDD.groupByKey();
```

JavaPairRDD<K, U> mapValues(Function<V, U> f)
Apply a user defined functin over the value of each pair of an input PairRDD and return a new PairRDD.
One pair is created in the returned PairRDD for each input pair, where the vaòue is obtained by applying the user-defined function on the value of the input pair.
The data type of the values of the new PairRDD can be different from the one of the input values.

```java
JavaPairRDD<String, Integer> nameAgePlusOneRDD = nameAgeRDD.mapValues(age -> new Integer(age+1));
```

JavaPairRDD<K, U> flatMapValues(Function<V, Iterable<U>> f)
Apply a user-defined function over the value of each pair of an input PairRDD and return a new PairRDD.
A list of pairs is created in the returned PairRDD for each input pair. The key of the created pairs is equal to the key of the input pair. The values of the created pairs are obtained by applying the user-defined function on the value of the input pair.
The data type of the values of the new PairRDD can be different from the data type of the values of the “input” PairRDD. The data type of the key is the same.

JavaRDD<K> keys()
Returns the list of keys of the input PairRDD. Duplicates are not removed.

JavaRDD<V> values()
Returns the list of values of the input RDD.

JavaPairRDD<K,V> sortByKey()
Return a new PairRDD obtained by sorting, in ascending order, the pairs of the input PairRDD by key.
The keys data type must be a class implementing the Ordered class.
! Shuffle operation !

```java
JavaPairRDD<String, Integer> sortedNameAgeRDD = nameAgeRDD.sortByKey();
```

## Set Transformations:

JavaRDD<T> union(JavaRDD<T> secondInputRDD) 
The result is the union of the two input RDDs. Duplicates are not removed

JavaRDD<T> intersection(JavaRDD<T> secondInputRDD)
The result is the intersection of the Two input RDDs. The output will not contain duplicates. 
! Shuffle operation !

JavaRDD<T> subtract(JavaRDD<T> secondInputRDD)
The result contains only the elements appearing in the RDD on which the subtract method is invoked.
Duplicates not removed
! Shuffle operation !

JavaPairRDD<T,R> cartesian(JavaRDD<R> secondInputRDD)
The two inputs RDDs can contain objects of two different data types.
The returned RDD is an RDD of pairs, containing all the combinations composed of one element of the first input RDD and one of the second.
! Enormous amount of data on the network ! Do not ever use it! 

**JavaPair:**

JavaPairRDD<K,V> subtractByKey(JavaPairRDD<K,U> other)
Create a new PairRDD containing only the pairs of the input PairRDD associated with a key that is not appearing as key in the pairs of the other PairRDD
The data type of the new PairRDD is the same of the “input” PairRDD. The input PairRDD and the other PairRDD must have the same type of keys.
The data type of the values can be different
! Shuffle ! 

```java
JavaPairRDD<String, Iterable<String>> subRDD = UgenRDD.subtractByKey(senListRDD);
```

JavaPairRDD <K, Tuple2<V,U>> join(JavaPairRDD<K,U>)
Join the key-value pairs of two PairRDDs based on the value of the key of the pairs. 
Each pair of the input PairRDD is combined with all the pairs of the other PairRDD with the same key.
The new PairRDD has the same key data type of the “input” PairRDDs and has a tuple as value (the pair of values of the two joined input pairs
The input PairRDD and the other PairRDD must have the same type of keys, but the data types of the values can be different
! Shuffle !

```java
JavaPairRDD<Integer, Tuple2<String, String>> joinPairRDD = questionsPairRDD.join(answersPairRDD);
```

JavaPairRDD <K, Tuple2<Iterable<V>, Iterable<U>>> cogroup(JavaPairRDD<K,U>)
Associates each key k of the input PairRDDs with the list of values associated with k in the input PairRDD and the list of values associated with k in the other PairRDD
The new PairRDD has the same key data type of the “input” PairRDDs, has a tuple as value (the two lists of values of the two input pairs)
The input PairRDD and the other PairRDD must have the same type of keys but the data types of the values can be different

! Shuffle ! 

```java
JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String> >> cogroupPairRDD = moviesPairRDD.cogroup(directorsPairRDD);
```

## Actions:

List<T> collect()
The collect actions returns a local Java list of objects containing the same objects of the considered RDD
Attention to the size of the list! Only use collect if it’s small, otherwise save in a file with saveAsTextFile method.

```java
JavaRDD<Integer> inputList = sc.parallelize(inputList);
List<Integer> retrievedValues = inputRDD.collect();
```

long count();
counts the number of lines of the input file

```java
long nLines = inputRDD.count();
```

java.util.Map<T, java.lang.Long> countByValue()
returns a local Java Map object containing the information about the number of times eache element occurs in the RDD
Attention to the size of the map! → attention to the number of names in this case.

```java
java.util.Map<String, java.lang.Long> namesOccurrences = neamesRDD.countByValue();
```

List<T> take(int n)

returns a local Java list of objects containing the first n elements of the considered RDD ← the order is consistent with the input

```java
JavaRDD<Integer> inputList = sc.parallelize(inputList);
List<Integer> retrivedValues = inputRDD.take(2);
```

T first()

returns a local Java object containing the first element of the considered RDD ← the order is consistent with the input
first() is different from take(1), because take(1) returns a list of one element.

```java
int retrivedValue = inputRDD.first();
```

List<T> top(int n, [comparator<T>])
returns a local Java list of object containing the top n largest elements of the RDD (default order: descending).

```java
List<Integer> retrivedValues = inputRDD.top(2, new myComparator());

public class myComparator implements Comparator<integer>, Serializable{
	@Override
	public int compare(Integer value1, Integer value2){
		return -1*(value1.compareTo(value2)); //return first the smallest one
	}
}
```

```java
JavaRDD<Double> IpAddRDD = sensorRDD.map(line -> Double.parseDouble(line.split(",")[2]));
//TOP 1
List<Double> maxPM10 = IpAddRDD.top(1);
System.out.println(maxPM10.get(0));

//TOP 3
JavaRDD<Double> IpAddRDD = sensorRDD.map(line -> Double.parseDouble(line.split(",")[2]));
List<Double> maxPM10 = IpAddRDD.top(3);
System.out.println(maxPM10.get(0) + " " + maxPM10.get(1) + " " + maxPM10.get(2));
```

List<T> takeOrdered(n, comparator<T>)
returns a local Java list containing the top n smallest elements of the input RDD

List<T> takeSample(boolean withReplacement, int n, [long seed])
returns a local list of Java list of objects containing n random elements of the considered RDD

```java
List<Integer> randomVal = inputRDD.takeSample(false, 2);
```

T reduce(Function2<T,T,T> combiner); 
returns a single object obtained combining the objects of the RDD using a user provided function that must be associative and commutative.

```java
Double maxPM10 = IpAddRDD.reduce((el1, el2) -> {
			if(el1 > el2) return el1;
			else return el2;
		});
```

T fold(T zeroValue, Function2<T, T, T> f)
returns a single Java object obtained by combining the objects of the RDD by using a user provided “function” that must be associative. An initial zer value must be specified

U aggregate(U zeroValue, Function2<U, T, U> seqOp, Function2<U,U,U> combOp
returns a single Java object obtained by combining the objects of the RDD and an initial zero value by using two user provided functions

```java
//Compute both the sum of the values occurring in the input RDD and the number of elements of the input RDD and 
//finaly store in a local Java variable of the driver the average computed over the values of the input RDD
// Define a class to store two integers: sum and numElements
class SumCount implements Serializable {
	public int sum;
	public int numElements;
	public SumCount(int sum, int numElements) {
		this.sum = sum;
		this.numElements = numElements;
	}
	public double avg() {
		return sum/ (double) numElements;
	}
}

// Create an RDD of integers. Load the values 1, 2, 3, 3 in this RDD
List<Integer> inputListAggr = Arrays.asList(1, 2, 3, 3);
JavaRDD<Integer> inputRDDAggr = sc.parallelize(inputListAggr);

// Instantiate the zero value - of the type I want for the return value
SumCount zeroValue = new SumCount(0, 0);
// Compute sum and number over the elements of inputRDDAggr
SumCount result = inputRDDAggr.aggregate(zeroValue, (a, e) -> {
	a.sum = a.sum + e;
	a.numElements = a.numElements + 1;
	return a;
}, (a1, a2) -> {
	a1.sum = a1. sum + a2.sum;
	a1.numElements = a1.numElements + a2.numElements;
	return a1;
} );
```

**JavaPair:**

java.util.Map<K, java.lang.Object> countBykey()
returns a local Java Map object containing the information about the number of elements associated with each key in the PairRDD
Attention to the number of distinct keys  of the PairRDD! If the number of distinct keys is large, the result of the action cannot be stored in a local variable of the Driver

```java
Map<String, Long> movieNumRatings = movieRatingRDD.countByKey();

//PER STAMPARE SU FILE! !throws IOException
File outputPath = new File(args[1]);
BufferedWriter bf = new BufferedWriter(new FileWriter(outputPath));
for(Map.Entry<String, Long> e : SensThre.entrySet()){
	System.out.println(e.getKey() + ", " + e.getValue());
	bf.write(e.getKey() + ", " + e.getValue()); 
	// new line 
	bf.newLine(); 
}
bf.flush(); 
bf.close();
```

java.util.Map<K, V> collectAsMap()
returns a local Java Map containing the same pairs of the considered PairRDD
Attention to the size of the PairRDD
Attention a Map cannot contain duplicate keys → if the input contains duplicates, only one is stored in the map, usually the last one in the PairRDD

java.util.List<V> lookup(K key) 
returns a local Java List containing the values of the pairs of the PairRDD associated with the key k specified as parameter
Attention to the size of the returned list.

```java
java.util.List<Integer> movieRatings = movieRatingsRDD.lookup("Forrest Gump");
```

## Domande

- [ ]  Quando usare aggregate e quando fold