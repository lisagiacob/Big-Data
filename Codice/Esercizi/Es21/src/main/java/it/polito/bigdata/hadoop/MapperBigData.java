package it.polito.bigdata.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
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
                    NullWritable> {// Output value type

    private ArrayList<String> stopWords;
    protected void setup(Context context) throws IOException, InterruptedException{
        String nextLine;
        stopWords=new ArrayList<String>();

        // Retrive the original paths of the distributed files
        URI[] urisCachedFiles = context.getCacheFiles();

        // Read and process the content of the file - 1st file in this case
        BufferedReader filestopwords = new BufferedReader(new FileReader(
            new File(new Path(urisCachedFiles[0].getPath()).getName())));
            
        // Iterate over the lines of the file
        while((nextLine = filestopwords.readLine()) != null){
            stopWords.add(nextLine);
	    }

	    filestopwords.close();
    }                    
            
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\\s+");
        String sentenceWithoutStopwords = new String("");

        for(String word :  words){
            if(stopWords.contains(word)==true) ;
            else sentenceWithoutStopwords=sentenceWithoutStopwords.concat(word+" ");

        }

        context.write(new Text(sentenceWithoutStopwords), NullWritable.get());
    }
}
