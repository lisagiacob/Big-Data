package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Use this mapper for the 2021 data
 * I send to the reducer only data for year 2021, one (codDC, A) kv pair for all the times the 
 * powre consumption was higher than 1000. I use 'A' to distinguish i the reducer the data from 2021 and 2020
 * The file is DailyPowerConsumption.txt
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
        
        String[] words = value.toString().split(",");
        String codDC = words[0];
        String date = words[1];
        String year = date.split("/")[0];
        Float pm = Float.parseFloat(words[2]);

        if(year.equals("2021") && pm > 1000) context.write(new Text(codDC), new Text("A"));
    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
    }
}
