package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Map.Entry;

import java.util.HashMap;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type

    HashMap<String, String> userRatingCount = new HashMap<String, String>();
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
            
    		/* Implement the map method */
            
            if(key.get() == 0) return;
            String words[] = value.toString().trim().split(",");
            String user = words[2];
            String art = words[1];
            Double rate = Double.parseDouble(words[6]);

            String stringa = art + "," + rate;

            context.write(new Text(user), new Text(stringa));  
    }
}
