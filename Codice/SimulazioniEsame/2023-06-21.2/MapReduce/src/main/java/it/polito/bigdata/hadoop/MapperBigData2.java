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
                    Text, // Input key type
                    IntWritable,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
                        
    protected void setup(Context context) {
    }
                        
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {
        String mid = key.toString();
        String part = value.toString();

        context.write(NullWritable.get(), new Text(mid + "," + part));
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
    }
}
