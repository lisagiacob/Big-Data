package it.polito.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
                        
    private MultipleOutputs<Text, NullWritable> mos = null;

    protected void setup(Context context){
        mos = new MultipleOutputs<Text, NullWritable>(context);
    }
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split(",");
        Float temp = Float.parseFloat(words[3]);

        if(temp<=30.0){
            mos.write("normalTemp" ,new Text(value), NullWritable.get());
        }
        else mos.write("highTemp" ,new Text(temp + ""), NullWritable.get());
    }
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
