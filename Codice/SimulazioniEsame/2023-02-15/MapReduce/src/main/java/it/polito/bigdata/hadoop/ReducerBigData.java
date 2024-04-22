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
                FloatWritable> {  // Output value type
    
    @Override
    // The reduce method is called once for each nation
    // (nation, city)
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        Map<String, Integer> cityH = new HashMap<String,Integer>();
        int n, sum = 0;
        float avg;

        for(Text v : values){
            if(cityH.containsKey(v.toString())){
                n = cityH.get(v.toString());
                cityH.put(v.toString(), n+1);
            }
            else cityH.put(v.toString(), 1); 
        }

        n = 0;

        for(String k :  cityH.keySet()){
            n += 1;
            sum += cityH.get(k);
        }

        avg = sum/n;
        if(avg > 10000) context.write(key, new FloatWritable(avg)); 
    }
}
