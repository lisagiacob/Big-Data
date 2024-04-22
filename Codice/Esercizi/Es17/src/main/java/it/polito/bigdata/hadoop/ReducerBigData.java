package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.avro.JsonProperties.Null;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type

    @Override
    // Since all the keys are null, the reducer is called only once
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        float max = Float.MIN_VALUE;

        System.out.println(key);
        for(FloatWritable value : values){
            if(value.get() > max) {
                max = value.get();
            }
        }

        // Emit the total number of occurrences of the current word
        context.write(new Text(key), new FloatWritable(max)); 
    }
}
