package it.polito.bigdata.hadoop;

import java.io.IOException;
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
                    NullWritable,         // Output key type
                    DateIncome> {// Output value type
                        
    private DateIncome top1;
    private DateIncome top2;

    protected void setup(Context context) {
        // for each mapper, top1 is used to store the information about the top1
        // date-income of the subset of lines analyzed by the mapper
        top1 = new DateIncome();
        top1.setIncome(Float.MIN_VALUE);
        top1.setDate(null);
    }
                        
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\\t");
        Float income = Float.parseFloat(words[1]);
        if(income > top1.getIncome() || (income == top1.getIncome() && words[0].compareTo(top1.getDate()) < 0)){
            top1.setDate(words[0]);
            top1.setIncome(income);
        }
        else if(Float.parseFloat(words[1]) > top2.getIncome() || (income == top2.getIncome() && words[0].compareTo(top2.getDate()) < 0)){
            top2.setDate(words[0]);
            top2.setIncome(income);
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
        context.write(NullWritable.get(), top1);
        context.write(NullWritable.get(), top2);
    }
}
