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
                        
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {
        
        String[] words = value.toString().split(",");
        String city = words[1];
        Float m2 = Float.parseFloat(words[3]);

        if(m2<60) context.write(new Text(city), new IntWritable(1));
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
    }
}
