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
                IntWritable> {  // Output value type
    
    //L'input sarà (null, (MID,numberOfPartecipants))
    //UN SOLO REDUCER
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        int maxP = 0;
        String TopMeet = "";

        for(Text v : values){
            String MID = v.toString().split(",")[0];
            int n = Integer.parseInt(v.toString().split(",")[1]);

            if(n > maxP) {
                maxP = n;
                TopMeet = MID;
            }
            else if(n == maxP){
                //If the number of partecipants is the same, I select the last meeting considering the alfabetical order
                if(MID.compareTo(TopMeet) > 0){
                    maxP = n;
                    TopMeet = MID; 
                }
            }
        }
        
        context.write(new Text(TopMeet), new IntWritable(maxP));
    }
}
