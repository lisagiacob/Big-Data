package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                IntWritable,           // Output key type
                IntWritable> {  // Output value type
    
    //L'input sarà (null, year,count) 
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        int maxCount = -1;
        int maxYear = 3000;
        for(Text v : values){
            int year = Integer.parseInt(v.toString().split(",")[0]);
            int count = Integer.parseInt(v.toString().split(",")[1]);

            if((count > maxCount) || (count == maxCount && year < maxYear)){
                maxCount = count;
                maxYear = year;
            }
        }

        context.write(new IntWritable(maxYear), new IntWritable(maxCount));
    }
}
