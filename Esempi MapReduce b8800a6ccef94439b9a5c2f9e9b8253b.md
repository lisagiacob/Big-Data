# Esempi MapReduce

### Exercise 29:

input 1:

A large textual file containing a set of records, where each line contains the info about one single user. Each line has the format: userID,Name,Surname,Gender,YearOfBirth,City,Education.

input 2: 

A large textual file with pairs (userID,MovieGenre), that mean that the user likes the movie genre.

output:

One record for each user that likes both Commedie and Adventures movies.
Each output record contains only Gender and YearOfBirth of the user.
Duplicates must not be removed

```java
class MapperType1BigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    	// Record format - Users table
    	// UserId,Name,Surname,Gender,YearOfBirth,City,Education
    	String[] fields=value.toString().split(",");
			
			String userId=fields[0];
			String gender=fields[3];
			String yearOfBirth=fields[4];
			
			// Key = userId
			// Value = U:+gender,yearOfBirth
			// U: is used to specify that this pair has been emitted by
			// analyzing the Users table
      context.write(new Text(userId), new Text("U:"+gender+","+yearOfBirth));
    }    
}
```

```java
class MapperType2BigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    		
    	// Record format - Likes table
			// UserId,MovieGenre
			String[] fields=value.toString().split(",");
		
			String userId=fields[0];
			String genre=fields[1];
		
			// Key = userId
			// Value = L
			// L: is used to specify that this pair has been emitted by
			// analyzing the likes file
			// Emit the pair if and only if the genre is Commedia or Adventure
			if (genre.compareTo("Commedia")==0 || genre.compareTo("Adventure")==0)
			{
				context.write(new Text(userId), new Text("L"));
			}
    }
}
```

```java
class ReducerBigData extends Reducer<Text, // Input key type
				Text, // Input value type
				NullWritable, // Output key type
				Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int numElements;
		String userData = null;

		// Iterate over the set of values and check if
		// 1) there are three elements (one related do the Users table and two
		// related to the Likes table
		// 2) store the information about the "profile/user" element

		numElements = 0;
		for (Text value : values) {

			String table_record = value.toString();

			numElements++;

			if (table_record.startsWith("U") == true) {
				// This is the user data record
				userData = table_record.replaceFirst("U:", "");
			}
		}

		// Emit a pair (null,user data) if the number of elements is equal to 3
		// (2 likes and 1 user data record)
		if (numElements == 3) {
			context.write(NullWritable.get(), new Text(userData));
		}
	}
}
```

### Exercise 28:

input 1:

A large textual file containing a set of questions, one for each line, with the format
questionId,TimeStamp,Text

input2: 

A large textual file containing a set of answers., one for each line, with the format 
answerId,QuestionId,TimeStamp,Text

output:

One line for each (question, answer) pair,, with the format 
QuestionId,TextQ,AnswerId,TextA

```java
class MapperType1BigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    	// Record format
    	// QuestionId,Timestamp,TextOfTheQuestion
   		String[] fields=value.toString().split(",");
			
			String questionId=fields[0];
			String questionText=fields[2];
			
			// Key = questionId
			// Value = Q:+questionId,questionText
			// Q: is used to specify that this pair has been emitted by
			// analyzing a question
            context.write(new Text(questionId), new Text("Q:"+questionId+","+questionText));
    }
}
```

```java
class MapperType2BigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    		
    	// Record format
			// AnswerId,QuestionId,Timestamp,TextOfTheAnswer
			String[] fields=value.toString().split(",");
		
			String answerId=fields[0];
			String answerText=fields[3];
			String questionId=fields[1];
		
			// Key = questionId
			// Value = A:+answerId,answerText
			// A: is used to specify that this pair has been emitted by
			// analyzing an answer
			context.write(new Text(questionId), 
										new Text("A:"+answerId+","+answerText));
    }
}
```

```java
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,  // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	String record;
    	ArrayList<String> answers=new ArrayList<String>();
    	String question=null;
 
      // Iterate over the set of values and store the answer records in 
    	// answers and the question record in question
        for (Text value : values) {
        	
        	String table_record=value.toString();
        	
        	if (table_record.startsWith("Q:")==true)
        	{	// This is the question record
        		record=table_record.replaceFirst("Q:", "");
        		question=record;
        	}
        	else
        	{	// This is an answer record
        		record=table_record.replaceFirst("A:", "");
        		answers.add(record);
        	}
        }

        // Emit one pair (question, answer) for each answer
        for (String answer:answers)
        {
        	context.write(NullWritable.get(), new Text(question+","+answer));
        }
    }
}
```

### Exercise 26:

input1: 

A large textual file containing a list of words per line

input 2:

The small file dictionary.txt containing the mapping of each possible word appearing in the first file with an integer. Each line contains the mapping of each possible word appearing in the first file, with an integer. word\tInteger\n

output:

A textual file containing the content of the large file, where the words are substituted by the corresponding integers.

```java
class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				NullWritable, // Output key type
				Text> {// Output value type

	private HashMap<String, Integer> dictionary;

	protected void setup(Context context) 
						throws IOException, InterruptedException {
		String line;
		String word;
		Integer intValue;

		dictionary = new HashMap<String, Integer>();
		// Open the dictionary file (that is shared by means of the distributed
		// cache mechanism)
		URI[] CachedFiles = context.getCacheFiles();

		// This application has one single single cached file.
		// Its path is URIsCachedFiles[0]
		BufferedReader fileStopWords = new BufferedReader(new FileReader(
																	 new File(CachedFiles[0].getPath())));

		// Each line of the file contains one mapping
		// word integer
		// The mapping is stored in the dictionary HashMap variable
		while ((line = fileStopWords.readLine()) != null) {

			// record[0] = integer value associated with the word
			// record[1] = word
			String[] record = line.split("\t");
			intValue = Integer.parseInt(record[0]);
			word = record[1];

			dictionary.put(word, intValue);
		}
		fileStopWords.close();
	}

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		String convertedString;
		Integer intValue;

		// Split each sentence in words. Use whitespace(s) as delimiter (=a
		// space, a tab, a line break, or a form feed)
		// The split method returns an array of strings
		String[] words = value.toString().split("\\s+");

		// Convert words to integers
		convertedString = new String("");

		// Iterate over the set of words
		for (String word : words) {

			// Retrieve the integer associated with the current word
			intValue = dictionary.get(word.toUpperCase());

			convertedString = convertedString.concat(intValue + " ");
		}
		// emit the pair (null, sentenceWithoutStopwords)
		context.write(NullWritable.get(), new Text(convertedString));
	}
}
```

### Exercise 25:

input: 

A textual file containing pairs of users, that represent their friendship. Username1,Username2

output:

A textual file containing one line for each user with at least one potential friend. Each line contains a user and the list of its potential friends.

```java
class MapperBigData extends Mapper<
                    LongWritable, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            LongWritable key, 	// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Extract username1 and username2
            String[] users = value.toString().split(",");
            
            // Emit two key-value pairs
            // (username1,username2)
            // (username2,username1)
            context.write(new Text(users[0]), new Text(users[1]));
            context.write(new Text(users[1]), new Text(users[0]));
    }
}
```

```java
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, 		// Input key type
        Iterable<Text> values, 	// Input value type
        Context context) throws IOException, InterruptedException {

    	HashSet<String> users;

    	// Each user in values is potential friend of the other users in values
    	// because they have the user "key" in common.
    	// Hence, the users in values are potential friends of each others.    	
    	// Since it is not possible to iterate more than one time on values
    	// we need to create a local copy of it. However, the 
    	// size of values is at most equal to the friend of user "key". Hence,
    	// it is a small list
    	users=new HashSet<String>();

      for (Text value : values) {
      	users.add(value.toString());
      }
    	
      // Compute the list of potential friends for each user in users
      for (String currentUser: users)
      {
      	String listOfPotentialFriends=new String("");
    	
        for (String potFriend: users) 
       	{	// If potFriend is not currentUser then include him/her in the 
       		// potential friends of currentUser
       		if (currentUser.compareTo(potFriend)!=0)
       			listOfPotentialFriends=listOfPotentialFriends.concat(potFriend+" ");
        }
        // Check if currentUser has at least one friend
       	if (listOfPotentialFriends.compareTo("")!=0)
        		context.write(new Text(currentUser), 
													new Text(listOfPotentialFriends));
        }
    }
}
```

```java
class MapperBigDataFilter extends Mapper<
                    Text, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    Text> {// Output value type

    protected void map(
            Text key, 	// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // Emit one key-value pair of each user in value.
	    	// Key is equal to the key of the input key-value pair
        String[] users = value.toString().split(" ");
            
        for (String user: users)
				{
        	context.write(new Text(key.toString()), new Text(user));
				}     
    }
}
```

```java
class ReducerBigDataFilter extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, 		// Input key type
        Iterable<Text> values, 	// Input value type
        Context context) throws IOException, InterruptedException {

    	String listOfPotentialFriends;
    	HashSet<String> potentialFriends;
    	
    	potentialFriends=new HashSet<String>();
    	
      // Iterate over the values and include the users in the final set
    	for (Text user: values)
      {
      	// If the user is new then it is inserted in the set
       	// Otherwise, it is already in the set, it is ignored
       	potentialFriends.add(user.toString());
      }

    	listOfPotentialFriends=new String("");
    	for (String user: potentialFriends)
    	{
    		listOfPotentialFriends=listOfPotentialFriends.concat(user+" ");
    	}

			context.write(new Text(key), new Text(listOfPotentialFriends));
    }
}
```

### Exercise 23bis:

input:

A textual file containing pairs of users, one per line.
One username specified via command line.

output:

The potential friends (only the potential friends!) of the specified username stored in a textual file - one single line

```java
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
    String us;
    String friends = new String("");

    protected void setup(Context context){
        us = context.getConfiguration().get("us");
    }     
            
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        String[] users = value.toString().split(",");
        
        if(users[0].equals(us)){
            if(friends.equals("")) friends = users[1];
            else friends = friends + " " + users[1];
        }
        else if(users[1].equals(us)){
            if(friends.equals("")) friends = users[0];
            else friends = friends + " " + users[0];
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
        context.write(new Text(friends), NullWritable.get());
    }
}
```

```java
class MapperBigData2 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    String us;
    String potentialFriends = new String(""), pairs = new String("");

    private String friends;
    protected void setup(Context context) throws IOException, InterruptedException{

        us = context.getConfiguration().get("user");
        String line;
        // Retrive the original paths of the distributed files
        URI[] urisCachedFiles = context.getCacheFiles();
        // Read and process the content of the file - 1st file in this case
        BufferedReader file = new BufferedReader(new FileReader(
                new File(new Path(urisCachedFiles[0].getPath()).getName())));
        // If the file isn't empty, save the line containing the user's friends
        if((line = file.readLine())!=null){
            // process the line
            friends = line;
            System.out.println(friends);
        }
        file.close();
    }
            
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        String[] users = value.toString().split(",");
        
        // Se nella riga non compare l'utente specificato, allora il pair è formato da amici/amici potenziali/sconosciuti
        if(!users[0].equals(us) && !users[1].equals(us)){
            if(friends.contains(users[0]) && !friends.contains(users[1])) potentialFriends = potentialFriends + " " + users[1];
            else if(friends.contains(users[1]) && !friends.contains(users[0])) potentialFriends = potentialFriends + " " + users[0]; 
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
        context.write(NullWritable.get(), new Text(potentialFriends));
    }
}
```