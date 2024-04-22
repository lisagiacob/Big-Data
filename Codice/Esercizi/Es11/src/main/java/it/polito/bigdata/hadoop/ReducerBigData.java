package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                PMVal,    // Input value type
                Text,           // Output key type
                PMVal> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<PMVal> values, // Input value type
        Context context) throws IOException, InterruptedException {

        int count = 0;
        float val = 0;
        PMVal local = new PMVal();

        for(PMVal value: values){
            val += value.getValue();
            count += value.getCount();
        }
        local.setValue(val);
        local.setCount(count);

        // Emit the total number of occurrences of the current word
        context.write(key, local); 
    }
}
