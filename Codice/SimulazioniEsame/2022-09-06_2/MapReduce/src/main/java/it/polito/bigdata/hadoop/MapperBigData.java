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
                    Text> {// Output value type
                        
    protected void setup(Context context) {
    }
                        
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {
        int pc = Integer.parseInt(value.toString().split(",")[2]);
        int year = Integer.parseInt(value.toString().split(",")[1].split("/")[0]);
        String DC = value.toString().split(",")[0];
        if(pc > 1000){
            context.write(new Text(DC), new Text(year + "-" + "1"));
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
    }
}
