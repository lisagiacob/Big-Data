package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
                IntWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    private int maxP;
    private Text maxMeet;
    @Override
    protected void setup (Context context){
        maxP = -1;
    }
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        int count = 0;
        for(IntWritable v : values){
            count +=1;
        }
        if(count > maxP || (count == maxP && key.toString().compareTo(maxMeet.toString()) > 0)){
            maxP = count;
            maxMeet = key;
        }
    }
    protected void cleanup (Context context) throws IOException, InterruptedException{
        context.write(maxMeet, new IntWritable(maxP));
    }
}
