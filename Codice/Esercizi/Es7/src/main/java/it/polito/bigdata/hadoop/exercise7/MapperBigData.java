package it.polito.bigdata.hadoop.exercise7;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				Text> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split each sentence in words. Use whitespace(s) as delimiter (=a
		// space, a tab, a line break, or a form feed)
		// The split method returns an array of strings
		String[] words = value.toString().split("\\s+");
		String sentence = words[0];

		// Iterate over the set of words
		for (String word : words) {
			// Transform word case
			String cleanedWord = word.toLowerCase();

			if(!word.contains("#") && !word.equals("and") && !word.equals("or") && !word.equals("not")){
				context.write(new Text(cleanedWord), new Text(sentence));
			}
		}
	}
}
