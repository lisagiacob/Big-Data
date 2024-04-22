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
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type

    String topCity;
    int topNum;
    protected void setup(Context context) {
        topCity = "";
        topNum = 0;
    }

    @Override
    // The reduce method is called once for each city
    // (city, 1)
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        //I save the name of the city we are currently reducing
        String city = values.toString().split(",")[0];
        int n = 0;
        for(Text v: values){
            n +=1;
        }
        if(n > topNum || n == topNum && city.compareTo(topCity) > 0){
             topNum = n;
             topCity = city;
        }
    }
    protected void cleanup(Context context) throws IOException, InterruptedException{
        context.write(new Text(topCity), NullWritable.get());
    }
}
