package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    private int maxCount;
    private int maxYear;

    protected void setup(Context context){
        
    }
    @Override
    // The reduce method is called once for each year 
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
            int count0 = 0;
            int count1 = 0;
            int year;
            for(Text v : values){
                year = Integer.parseInt(v.toString().split("-")[0]);
                if(year == 2020) count0 += 1;
                else if(year == 2021) count1 += 1;
            }
            if(count1 > count0) context.write(key, new IntWritable(count1));
    }
    protected void cleanup(Context context) throws IOException, InterruptedException{
    } 
}
