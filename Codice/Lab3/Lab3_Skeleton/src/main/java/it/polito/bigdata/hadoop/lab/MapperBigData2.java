package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    WordCountWritable> {// Output value type

    TopKVector<WordCountWritable> top100;
    
@Override
protected void setup(Mapper<LongWritable, Text, NullWritable, WordCountWritable>.Context context)
        throws IOException, InterruptedException {

    top100 = new TopKVector<WordCountWritable>(100);
    
    super.setup(context);
}

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String words[] = value.toString().trim().split("\\t");
            Integer val = Integer.parseInt(words[1]);

            top100.updateWithNewElement(new WordCountWritable(words[0], val));
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, NullWritable, WordCountWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        for(WordCountWritable it : top100.getLocalTopK()){
            context.write(NullWritable.get(), it);
        }
                
        super.cleanup(context);
    }
}
