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
                    AvgWritable> {// Output value type
    
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] words = value.toString().split("\\t");
            AvgWritable val = new AvgWritable();
            // Transform word case
            String date = words[0].toLowerCase().split("\\-")[0] + "-" + words[0].toLowerCase().split("\\-")[1];
            Float income = Float.parseFloat(words[1]);

            val.setSum(income);
            val.setCount(1);
          
            context.write(new Text(date), val); 
    }
}
