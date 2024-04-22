package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.hadoop.lab.DriverBigData.COUNTERS;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        String str = context.getConfiguration().get("str");

        /* Implement the map method */
        
        // Split each sentence in words. Use whitespace(s) as delimiter 
        // (=a space, a tab, a line break, or a form feed)
        // The split method returns an array of strings
        String[] words = value.toString().split("\\s+");
        IntWritable val = new IntWritable(Integer.parseInt(words[1]));

        if(words[0].startsWith(str)){
            // emit the pair (word, 1)
            context.write(new Text(words[0]), val);
            context.getCounter(COUNTERS.COUNTED).increment(1);
        }
        else context.getCounter(COUNTERS.IGNORED).increment(1);
    }
}
