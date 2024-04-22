/* Set package */
package it.polito.bigdata.hadoop;

/* Import libraries */
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/* Combiner Class */
class CombinerBigData extends 
	Reducer<Text, // Input key type 
	StatisticsWritable, // Input value type 
	Text, // Output key type
	StatisticsWritable>{ // Output value type
	@Override
	/* Implementation of the reduce method */ 
	protected void reduce( Text key, // Input key type
	Iterable<StatisticsWritable> values, // Input value type
	Context context) throws IOException, InterruptedException {
		float tMin, min = 1000;
		float tMax, max = 0;
		for(StatisticsWritable value: values){
			tMin = value.getMin();
			tMax = value.getMax();

			if(tMin < min) min = tMin;
			if(tMax > max) max = tMax;
		}

		StatisticsWritable local = new StatisticsWritable();

		local.setMin(min);
		local.setMax(max);
		
		// Emit the total number of occurrences of the current word
		context.write(key, local); 
		
	} // End reduce method
} // End of class WordCountCombiner