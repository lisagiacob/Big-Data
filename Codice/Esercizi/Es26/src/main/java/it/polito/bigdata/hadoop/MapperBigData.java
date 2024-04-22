package it.polito.bigdata.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type

    private HashMap<String, Integer> ints;
    protected void setup(Context context) throws IOException, InterruptedException{
        String IntWord;
        String[] words;

        ints = new HashMap<String, Integer>();

        // Retrive the original paths of the distributed files
        URI[] urisCachedFiles = context.getCacheFiles();

        // Read and process the content of the file - 1st file in this case
        BufferedReader fileintwords = new BufferedReader(new FileReader(
            new File(new Path(urisCachedFiles[0].getPath()).getName())));
            
        // Iterate over the lines of the file
        while((IntWord = fileintwords.readLine()) != null){
            words = IntWord.split("\t");
            ints.put(words[1].toUpperCase(), Integer.parseInt(words[0]));
	    }

	    fileintwords.close();
    }                    
            
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\\s+");
        String sentenceOfInts = new String("");

        for(String word :  words){
            sentenceOfInts = sentenceOfInts + ints.get(word) + " ";
        }

        context.write(new Text(sentenceOfInts), NullWritable.get());
    }
}
