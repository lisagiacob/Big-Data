package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MapReduce program
 */
public class DriverBigData extends Configured 
implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Path inputPath;
    Path outputDir, outputDir2;
    int numberOfReducers;
	  int exitCode;  
  
	  // Parse the parameters
	  // Number of instances of the reducer class 
    numberOfReducers = Integer.parseInt(args[0]);
    // Folder containing the input data
    inputPath = new Path(args[1]);
    // Output folder
    outputDir = new Path(args[2]);
    outputDir2 = new Path(args[3]);
    
    Configuration conf = this.getConf();
    conf.set("us", args[4]);

    // Define a new job
    Job job = Job.getInstance(conf); 

    // Assign a name to the job
    job.setJobName("Es23");
    
    // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
    FileInputFormat.addInputPath(job, inputPath);

    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job, outputDir);
    
    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);
    
    // Set job input format
    job.setInputFormatClass(TextInputFormat.class);

    // Set job output format
    job.setOutputFormatClass(TextOutputFormat.class);

    // Set map class
    job.setMapperClass(MapperBigData.class);
    
    // Set map output key and value classes
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
   
    /* 
    //Set combine class
    job.setCombinerClass(CombinerBigData.class);

    // Set combine output classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    */

    // Set reduce class
    //job.setReducerClass(ReducerBigData.class);
        
    // Set reduce output key and value classes
    //job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(FloatWritable.class);

    // Set number of reducers
    job.setNumReduceTasks(0); 
    
    // Execute the job and wait for completion
		if (job.waitForCompletion(true) == true){
    	// Execute the second job to find the potential friends of the user of interest
			// based on the list of its friends (that is the output of the previous job and
			// it is shared by using the distributed cache)
			// The shared file is also used to remove the users who are already friends 
			// of the user of interest
			Configuration conf2 = this.getConf();
			conf2.set("user", args[4]);

			// Define a new job
			Job job2 = Job.getInstance(conf2);

			// Add the hdfs file created by the first job (outputDir/part-r-00000)
			// in the distributed cache.
			// It contains the list of friends of the user we are interest in.
			job2.addCacheFile(
				new Path(outputDir + "/part-m-00000").toUri());

			// Assign a name to the job
			job2.setJobName("Exercise #23 - Job 2");

			// Set path of the input file/folder
			// The input path is the same input path of the first job
			FileInputFormat.addInputPath(job2, inputPath);

			// Set path of the output folder for this job
			FileOutputFormat.setOutputPath(job2, outputDir2);

			// Specify the class of the Driver for this job
			job2.setJarByClass(DriverBigData.class);

			// Set job2 input format
			job2.setInputFormatClass(TextInputFormat.class);

			// Set job2 output format
			job2.setOutputFormatClass(TextOutputFormat.class);

			// Set map class
			job2.setMapperClass(MapperBigData2.class);

			// Set map output key and value classes
			job2.setMapOutputKeyClass(NullWritable.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setNumReduceTasks(0);

			// Execute the job and wait for completion
			if (job2.waitForCompletion(true) == true)
				exitCode = 0;
			else
				exitCode = 1;
    }
		else
			exitCode = 1;
		return exitCode;
	}
  
  public static void main(String args[]) throws Exception {
	// Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), 
    		new DriverBigData(), args);

    System.exit(res);
  }
}