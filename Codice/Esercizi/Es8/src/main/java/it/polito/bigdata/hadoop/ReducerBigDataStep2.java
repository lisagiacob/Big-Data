package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigDataStep2 extends Reducer<
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

        System.out.println(key);
        AvgWritable local = new AvgWritable();

        for(AvgWritable value: values){
            for(int i=1; i<13; i++){
                if(value.getMonth() == i){
                    totSum += value.getSum();
                    totCount += 1;
                    if(totCount != 0){
                        System.out.println("Month:" + value.getMonth() + " income:" + totSum);
                        local.setSum(totSum);
                        local.setCount(totCount);
                    }
                }
            }
        }

        local.setYear();
        
        // Emit the total number of occurrences of the current word
        context.write(key, new Text(local.toString())); 
    }
}
