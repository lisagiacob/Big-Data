package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
    String us;
    String friends = new String("");

    protected void setup(Context context){
        us = context.getConfiguration().get("us");
    }     
            
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        String[] users = value.toString().split(",");
        
        if(users[0].equals(us)){
            if(friends.equals("")) friends = users[1];
            else friends = friends + " " + users[1];
        }
        else if(users[1].equals(us)){
            if(friends.equals("")) friends = users[0];
            else friends = friends + " " + users[0];
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
        context.write(new Text(friends), NullWritable.get());
    }
}
