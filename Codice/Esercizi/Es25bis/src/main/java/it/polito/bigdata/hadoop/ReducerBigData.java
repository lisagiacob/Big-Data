package it.polito.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type


    @Override
    // Since all the keys are null, the reducer is called only once
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        String friends = "";

            for(Text user : values){
                if(!user.equals(key) && !friends.contains(user +"")) {
                    friends = friends + " " + user;
                }
            }

            context.write(key, new Text(friends));
        }
}
