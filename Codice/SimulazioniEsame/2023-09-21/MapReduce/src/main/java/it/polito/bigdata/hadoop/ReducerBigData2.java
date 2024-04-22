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
                Text,           // Output key type
                Text> {  // Output value type
    
    //L'inputsarà (SID, (L || S)) 
    @Override
    // The reduce method is called once for each season (key = SID,SeasonNum)
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        int l = 0, s = 0;
        
        for(Text v : values){
            if(v.toString().equals("L")) l += 1;
            else if (v.toString().equals("S")) s += 1; 
        }

        // ((SID, nSeason), nEp)
        context.write(key, new Text("long seasons: " + l + " short seasons: " + s)); 
    }
}
