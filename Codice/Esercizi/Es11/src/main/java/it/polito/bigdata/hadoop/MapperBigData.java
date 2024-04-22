package it.polito.bigdata.hadoop;

import java.io.IOException;
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
                    PMVal> {// Output value type
    	
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        PMVal PM10 = new PMVal();
        String[] words = value.toString().split(",");
        String sensor = words[0];
        PM10.setValue(Float.parseFloat(words[2]));
        PM10.setCount(1);
        context.write(new Text(sensor), PM10);
    }
}
