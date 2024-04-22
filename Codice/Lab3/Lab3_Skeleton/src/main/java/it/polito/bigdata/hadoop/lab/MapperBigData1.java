package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        /* Implement the map method */
        String words[] = value.toString().trim().split(",");

        // Iterate over the set of words
        for(int j=1; j < words.length-1; j++){
            String w1 = words[j];
            for(int i=j+1; i < words.length; i++) {
                String w2 = words[i];   
                String Word; 
                if(w1.compareTo(w2)<0){
                    Word = w1 + " " + w2;
                }
                else Word = w2 + " " + w1;
                
                context.write(new Text(Word), new IntWritable(1));            
            }
        }
    }
}
