package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                AvgWritable,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<AvgWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        float totSum = 0;
        int totCount = 0; 

        for(AvgWritable value: values){
            totSum += value.getSum();
            totCount += value.getCount();
        }

        AvgWritable local = new AvgWritable();

        local.setSum(totSum);
        local.setCount(totCount);
        
        // Emit the total number of occurrences of the current word
        context.write(key, new Text(local.toString())); 
    }
}
