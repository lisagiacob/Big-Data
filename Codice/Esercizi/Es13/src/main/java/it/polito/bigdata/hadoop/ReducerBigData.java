package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                DateIncome,    // Input value type
                NullWritable,           // Output key type
                DateIncome> {  // Output value type
    
    @Override
    // Since all the keys are null, the reducer is called only once
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<DateIncome> values, // Input value type
        Context context) throws IOException, InterruptedException {

        DateIncome GlobalTop1 = new DateIncome();
        GlobalTop1.setDate(null);
        GlobalTop1.setIncome(Float.MIN_VALUE);
        String date;
        Float income;

        for(DateIncome value: values){
            date = value.getDate();
            income = value.getIncome();
            if(income > GlobalTop1.getIncome() || (income == GlobalTop1.getIncome() && date.compareTo(GlobalTop1.getDate()) < 0)){
                GlobalTop1.setDate(date);
                GlobalTop1.setIncome(income);
            }

        }

        // Emit the total number of occurrences of the current word
        context.write(NullWritable.get(), GlobalTop1); 
    }
}
