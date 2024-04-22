package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        /**
         * Come input avrò: (word, [1,...,1])
         * Come output voglio: (word, n)
         */

        int occurrences = 0;

        // Iterate over the set of values and sum them 
        for (IntWritable value : values) {
            occurrences = occurrences + value.get();
        }
        
        context.write(key, new IntWritable(occurrences));
    }
}
