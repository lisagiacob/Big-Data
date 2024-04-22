package it.polito.bigdata.hadoop;

import java.io.IOException;
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
                    Text> {// Output value type
    
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] words = value.toString().split("[\\s+,]");
            
            
            // Transform word case
            String sensor = words[0].toLowerCase();
            Double pm = Double.parseDouble(words[2]);

            if(pm>=50) { // emit the pair (word, 1)            
                context.write(new Text(sensor),
                new Text(words[1])); 
            }
    }
}
