package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigDataStep3 extends Reducer<
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
        int[] totCount = {0,0,0,0,0,0,0,0,0,0,0,0}; 
        int c = 0;
        
        for(AvgWritable value: values){
            for(int i=0; i<12; i++){
                if(value.getMonth() == i+1){
                    totSum += value.getSum();
                    totCount[i] += value.getCount();
                }
            }
        }

        for(int i=0; i<12; i++){
            if(totCount[i] != 0)
            {
                c += 1;
            }
        }

        AvgWritable local = new AvgWritable();

        local.setSum(totSum);
        local.setCount(c);
        local.setYear();
        
        // Emit the total number of occurrences of the current word
        context.write(key, new Text(local.toString())); 
    }
}
