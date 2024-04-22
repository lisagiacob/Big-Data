package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                WordCountWritable,    // Input value type
                NullWritable,           // Output key type
                WordCountWritable> {  // Output value type
    TopKVector<WordCountWritable> top100;
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<WordCountWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        top100 = new TopKVector<WordCountWritable>(100);

        for(WordCountWritable it : values){
            top100.updateWithNewElement(new WordCountWritable(it));
        }

        for(WordCountWritable it : top100.getLocalTopK()){
            context.write(NullWritable.get(), it);
        }
    	
    }
}
