# Spark

## Resilient Distributed Datasets - RDDs

Datas are represented as RDDs, which are distributed colloection of objects spread across the nodes of a cluster and split in patitions, each node of the cluster that is running an application contains at least one partition of the RDDs defined in the application.
RDDs allow the parallel execution of the code invoked on them.

Spark programs are written in terms of operations on resilient distributed data sets.

RDDs are built and manipulated through a set of parallel transformations and actions.

RDDs are immutable once constructed ← it is needed to recompute lost data.

RDDs can be created by parallelizing existing collections of the hosting programming lenguage,
from large files stored in HDFS,
from files stored in traditional file systems,
by transforming an existing RDDs.

```java
//RDDs form files
JavaRDD<String> lines = sc.textFile(inputFile);

//No computeation occurs when sc.textFile(String inPath, <int numPartitions>) is invoked!
//The data is ONLY read from the input file when it's needed
//(ex. when an action is applòied on the linesRDD or one of it's descendant)

//RDDs from a local Java collection
List<String> inputList = Arrays.asList("First element", "Second element", "Third element");
JavaRDD<String> distList = sc.parallelize(inputList);

//No computeation occurs when sc.parallelize(List<T> list, <int numPartitions>) is invoked!
//The data is ONLY read from the input file when it's needed
```

An RDD can be easily stored in a textual (HDFS) file

```java
lines.saveAsTextFile(outputPath);

//The content of the lines is computed when saveAsTextFile() is invoked
//Spark computes the content associated with an RDD only when the content is needed
```

The content of an RDD can be retrived from the nodes of the cluster and stored in a local Java variable in the Driver.
One must pay attention to the size of the RDD! Large RDDs cannot be stored in local variable of the Driver, those are stored in the main memory!

```java
List<String> contentOfLines = lines.collect();
```

## Transformations and Actions

RDD support two types of operations: Transformations and actions

**Transformations** are operations on RDDs that return a new RDD ← RDDs are immutable!
Transformations are computed lazily → only executed when an action is applied on the RDDs generated.
When a transformation is invoked spark keeps track of the dependency between the input RDD and the new RDD returned by the transformation. The content of the new RDD is not computed.

The lineage graph represents the information about which RDDs are used to create a new RDD.
It’s needed to compute the content of an RDD the first time an action is invoked on it.

**Actions** are operations that:

1. Return results to the Driver program (ex. return local Java variables
Attention to the size of the returned results!
2. Write the result in the storage (output file/folder)

### Passing “functions” to transformations and actions

Many transformations and some actions are based on user provided functions that specify which “transformation” function must be applied on each element of the input RDD.

Some basic transformation analyze the content of one single RDD and return a new RDD
(e.g. filter, map, flatMap, distinct, sample)
Some other transformations analyze the content of two input RDDs and return a new RDD
(e.g. union, intersection, substract, cartesian)

## Pair RDDs

Spark supports also RDDs of key-value pairs.
Pair RDDs are characterized by specific operations, but are also characterized also by the operations available for the standard RDDs, where the specified functions operate on tuples.

Pair RDDs allow to group data by key and to perform computation by key.
The basic idea is similar to the one of the MapReduce based programs.

Pair RDDs can be built from regular RDDs by apliyng the mapToPair and the flatMapToPair transformations, from other pair RDDs by applying specific transformations, and from a Java in-memory collection by using the parallelizePairs method.

Pairs are represented as tuples composed of two elements: key and value. To rappresent tuples Java exploits the scala.Tuple2<K, V> class.

new Tuple2(key, value) can be used to instance a new object of type Tuple2 in Java.
The two elements can be retrieved by using the methods ._1() and ._2()

Spark supports also some transformations on two PairRDDs.

## Persistence and Cache

Spark computes the content of an RDD each time an action is invoked on it.
If the same RDD is used multiple times in an application, Spark recomputes its content every time an action is invoked on the RDD, or on one of it’s “descendants” ← expensive.
We can ask Spark to persist/cache RDDs.

When Spark cache an RDD, each node stores the content of its partitions in memory and reuses them in other actions on that RDD and on its descendant.
The first time the content of a cached RDD is computed in an action, it will be kept in the main memory of the nodes, the next actions on the same RDD will read its content from memory.

Spark supports several storage levels.
The storage level is used to specify if the content of the RDD is stored in the main memory of the nodes, on the local disk of the nodes, or partially in the main memory and partially on the disk.

| MEMORY_ONLY | Store RDD as deserialized Java objects in the JVM. 
If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed. This is the default level. |
| --- | --- |
| MEMORY_AND_DISK | Store RDD as deserialized Java objects in the JVM. 
If the RDD does not fit in memory, store the partitions that don't fit on (local) disk, and read them from there when they're needed. |
| MEMORY_ONLY_SER | Store RDD as serialized Java objects (one byte array per partition). 
This is generally more space-efficient than deserialized objects, especially when using a fast
serializer, but more CPU-intensive to read. |
| MEMORY_AND_DISK_SER | Similar to MEMORY_ONLY_SER, but spill partitions that don't fit in memory to disk instead of recomputing them on the fly each time they're needed. |
| DISK_ONLY | Store the RDD partitions only on disk. |
| MEMORY_ONLY_2
MEMORY_AND_DISK_2
… | Same as the levels above, but replicate each partition on
two cluster nodes.
If one node fails, the other one can be used to perform the actions on the RDD without recomputing the content of the RDD. |
| OFF_HEAP | Similar to MEMORY_ONLY_SER, but store the data in off-heap memory. 
This requires off-heap memory to be enabled. |

An RDD can be marked to be persistent by using the 
JavaRDD<T> persist(StorageLevel level) method     ← ex. StorageLevel.MEMORY_ONLY()

You can cache an RDD by using the
JavaRDD<T> cache() method
It corresponds to persist the RDD with the storage level ‘MEMORY_ONLY’ ← equivalent to 
inRDD.persist(StorageLevel.MEMORY_ONLY())

Both persist and cache return a new JavaRDD ← because RDDs are immutable

The storage levels that store RDDs on disk are useful if and only if:

1. The size of the RDD is significantly smaller than the size of the input dataset
2. The functions that are used to compute the content of the RDD are expensive

Spark automatically monitors cache usage in each node and drops out old data partitions in a least-recently-used fashion.
An RDD can be manually removed from the cache by using the
JavaRDD<T> unpersist() method

## Accumulators

When a function passed to a Spark operation is executed on a remote cluster node, it works on separate copies of all the variables used in the function.
These variables are copied to each node of the cluster, and no updates to the variables on the nodes are propagated back to the driver program.

Spark provides a type of shared variables called accumulators, which are only added to through an associative operation and can therefore be efficiently supported in parallel → useful to implement counters.

The driver defines and initializes accumulators and the code executed in the worker nodes increases the their values.
The final values are then returned to the driver node, which is the only one that can access the final value of each accumulator.

When the accumulators are updated inside transformations, it must be noted that transformations are lazily evaluated and so the accumulators are update only when (and if) there is an action that triggers the execution of the transformations.
To assure that the values of the accumulators are correct, they must be updated in transformations or actions that are executed exactly one time (or at least the exact number of times that is needed)

The foreach method is an action and is frequently used to update accumulators.
It applies a function to all elements of an RDD.
It returns no values and is usually used to print/store the content of an RDD and to update accumulators.
public void foreach(Function<T> f)

Spark natively supports accumulators of numeric types (Long, Double) but programmers can add support for new data types

```java
final LongAccumulator invalidEmails = sc.sc().longAccumulator();
...
JavaRDD<String> validEmailsRDD = emailsRDD.filter(line -> {
	if(line.contains("#") == false) invalidEmails.add(1);
	return line.contains("@");
});
...
System.out.println("Invalid emails: " + invalidEmails.value());
```

```java
final LongAccumulator invalidEmails = sc.sc().longAccumulator();
...
emailsRDD.foreach(line -> {
	if(line.contains("@") == false) invalidEmails.add(1);
})
...
System.out.println("Invalid emails: " + invalidEmails.value());
```

## Broadcast variables

Spark supports also broadcast variables, which are read-only shared variables instantiated on the driver.
A copy of each variable is sent only one time to each one of the tasks executing a Spark action using the variable.

Broadcast variables are usually used to share large lookup tables.
Broadcast <T> broadcast(T value)

```java
// Read the content of the dictionary from the first file and
// map each line to a pair (word, integer value)
JavaPairRDD<String, Integer> dictionaryRDD = sc.textFile("dictionary.txt").mapToPair(line ->
{
	String[] fields = line.split(" ");
	String word=fields[0];
	Integer intWord=Integer.parseInt(fields[1]);
	return new Tuple2<String, Integer>(word, intWord) ;
});

// Create a local HashMap object that will be used to store the
// mapping word -> integer
HashMap<String, Integer> dictionary = new HashMap<String, Integer>();

// Create a broadcast variable based on the content of dictionaryRDD
// Pay attention that a broadcast variable can be instantiated only
// by passing as parameter a local java variable and not an RDD.
// Hence, the collect method is used to retrieve the content of the
// RDD and store it in the dictionary HashMap<String, Integer> variable
for (Tuple2<String, Integer> pair: dictionaryRDD.collect()) {
	dictionary.put(pair._1(), pair._2());
}
final Broadcast<HashMap<String, Integer>> dictionaryBroadcast = sc.broadcast(dictionary);

// Read the content of the second file
JavaRDD<String> textRDD = sc.textFile("document.txt");

// Map each word in textRDD to the corresponding integer and concatenate them
JavaRDD<String> mappedTextRDD= textRDD.map(line -> {
	String transformedLine=new String("");
	Integer intValue;
	// map each word to the corresponding integer
	String[] words=line.split(" ");
	for (int i=0; i<words.length; i++) {
		intValue=dictionaryBroadcast.value().get(words[i]);
		transformedLine=transformedLine.concat(intValue+" ");
	}
	return transformedLine;
});

// Store the result in an HDFS file
mappedTextRDD.saveAsTextFile(outputPath);
```