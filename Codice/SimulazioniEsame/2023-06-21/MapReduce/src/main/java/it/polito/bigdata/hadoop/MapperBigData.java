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
                  
    //l'input è dal file invitations.txt
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {
        
        String[] words = value.toString().split(",");

        //se l'invito è accettato (Yes) allora scrivo in output (MID, 1)
        if(words[2].equals("Yes")) context.write(new Text(words[0]), new IntWritable(1));
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
    }
}
