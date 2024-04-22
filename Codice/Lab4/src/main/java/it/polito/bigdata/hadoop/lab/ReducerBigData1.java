package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,           // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        LinkedList<String> lista = new LinkedList<String>();
        Double rates = 0.0;
        int count = 0;

        for(Text value : values){
            lista.add(value.toString());
            String words[] = value.toString().trim().split(",");
            Double rate = Double.parseDouble(words[1]);

            rates += rate;
            count += 1;
            
        }

        Double mean = rates/count;

        for(String val : lista){
            String words[] = val.toString().trim().split(",");
            Double rate = Double.parseDouble(words[1]);
            rate = rate - mean;
            context.write(new Text(words[0]), new DoubleWritable(rate));
        }
    }
}
