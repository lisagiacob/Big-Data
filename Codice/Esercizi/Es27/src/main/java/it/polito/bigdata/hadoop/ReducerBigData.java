package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.avro.JsonProperties.Null;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                NullWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type

    
    int wordId;
    protected void setup(){
        wordId = 0;
    }

    @Override
    // Since all the keys are null, the reducer is called only once
    protected void reduce(
        Text key, // Input key type
        Iterable<NullWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        wordId++;

        // Emit the total number of occurrences of the current word
        context.write(key, new IntWritable(wordId)); 
    }
}
