package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    IntWritable,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
                        
    protected void setup(Context context) {
    }
    // input: ((SID, nSeason), nEp) - un input per ogni stagione di ogni serie.
    protected void map(
        Text key,   // Input key type
        IntWritable value,         // Input value type
        Context context) throws IOException, InterruptedException {
        
        String[] words = key.toString().split(",");

        //L'output sarà (SID, (L || S)) 
        if(value.get() > 10) context.write(new Text(words[0]), new Text("L"));
        else context.write(new Text(words[0]), new Text("S")); 
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
    }
}
