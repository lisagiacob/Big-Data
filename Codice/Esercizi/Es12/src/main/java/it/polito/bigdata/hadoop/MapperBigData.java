package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    FloatWritable> {// Output value type

    float threshold;

    protected void setup(Context context) {
        // I retrieve the value of the threshold only one time for each mapper
        threshold = Float.parseFloat(context.getConfiguration().get("Threshold"));
    }
    	
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\\t");
        String sensorDate = words[0];
        Float PM10 = Float.parseFloat(words[1]);
        if(PM10 < threshold){
            context.write(new Text(sensorDate), new FloatWritable(PM10));
        }
    }
}
