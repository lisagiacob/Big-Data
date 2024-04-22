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
                NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    // The reduce method is called once for each nation
    // (nation, city)
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        int maxP = -1;
        String maxM;
        String meet;
        int count;

        for(Text v : values){
            meet = v.toString().split(",")[0];
            count = Integer.parseInt(v.toString().split(",")[1]);

            if(count > maxP || (count == maxP && key.toString().compareTo(maxM.toString()) > 0)){
                maxP = count;
                maxM = key;
            }
        }
        context.write(new Text(maxM), NullWritable.get());
    }
}
