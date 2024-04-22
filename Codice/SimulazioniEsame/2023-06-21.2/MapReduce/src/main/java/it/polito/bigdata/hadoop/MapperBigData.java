package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
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
                    IntWritable> {// Output value type
                        
    protected void setup(Context context) {
    }
    //invitations file 
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {
            String MID = value.toString().split(",")[0];
            String a = value.toString().split(",")[2];

            if(a.equals("Yes")) context.write(new Text(MID), new IntWritable(1)); 
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
    }
}
