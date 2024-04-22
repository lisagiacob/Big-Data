package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                StatisticsWritable,    // Input value type
                Text,           // Output key type
                StatisticsWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<StatisticsWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        // Iterate over the set of values and sum them 
        int count = 0;
		float sum = 0;
		for(StatisticsWritable stat: values){
			count += stat.getCount();
			sum += stat.getSum();
		}

		StatisticsWritable local = new StatisticsWritable();

		local.setCount(count);
		local.setSum(sum);
		
        context.write(key, local);
    }
}
