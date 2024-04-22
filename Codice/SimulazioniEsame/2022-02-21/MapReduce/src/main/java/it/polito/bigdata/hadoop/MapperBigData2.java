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
class MapperBigData2 extends Mapper<
                    IntWritable, // Input key type
                    IntWritable,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
                        
    protected void setup(Context context) {
    }
    // input: ((SID, nSeason), nEp) - un input per ogni stagione di ogni serie.
    protected void map(
        IntWritable key,   // Input key type
        IntWritable value,         // Input value type
        Context context) throws IOException, InterruptedException {
        IntWritable year = key;
        IntWritable count = value;

        context.write(NullWritable.get(), new Text(year +","+ count));
        
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
    }
}
