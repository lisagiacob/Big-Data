package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] words = value.toString().trim().split("\\s+");

            
            // Iterate over the set of words
            String temp = words[0].toLowerCase();
            for(String word : words) {
                if(word == words[0]) continue;
            	// Transform word case
                String cleanedWord = temp + " " + word.toLowerCase();

                temp = word.toLowerCase();
                
                // emit the pair (word, 1)
                context.write(new Text(cleanedWord),
                		      new IntWritable(1));
            }
    }
}
