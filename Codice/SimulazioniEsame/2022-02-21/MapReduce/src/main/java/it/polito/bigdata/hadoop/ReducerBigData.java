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
                IntWritable,    // Input value type
                IntWritable,           // Output key type
                IntWritable> {  // Output value type
    
    private int maxCount;
    private int maxYear;

    protected void setup(Context context){
        maxCount = -1;
        maxYear = 3000;
    }
    @Override
    // The reduce method is called once for each year 
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
        int year = Integer.parseInt(key.toString());
        int count = 0;
        for(IntWritable v : values){
            count +=1;
        }

        if((count > maxCount) || (count == maxCount && year < maxYear)){
            maxCount = count;
            maxYear = year;
        }
    }
    protected void cleanup(Context context) throws IOException, InterruptedException{
        context.write(new IntWritable(maxYear), new IntWritable(maxCount));
    } 
}
