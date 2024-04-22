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
 * I use the values (A or B) to distinguish the data from 2021 and 2020
 * I calculate the number of times there was an high consuption for both cases
 * and only keep the DC where the cases for A where higher than those of B
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    // The reduce method is called once for each DC
    // (codDC, A||B)
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        String letter;
        int n1 = 0, n0 = 0;

        for(Text v: values){
            letter = values.toString();
            if(letter.equals("A")){
                //2021
                n1 += 1;
            }
            else{
                //2020
                n0 += 0;
            }
        }

        if(n1 > n0) context.write(key, NullWritable.get());
    }
}
