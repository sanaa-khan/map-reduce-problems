// Sana Ali Khan
// 18i-0439

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class avgLength {
	
	public static void main(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.err.println("Usage: avgLength <input path> <output path>");
	      System.exit(-1);
	    }
	    
	    // configuration and job
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Average Length");
	    
	    // set job properties
	    job.setJarByClass(avgLength.class);
	    job.setJobName("AverageLength");
	    
	    // set input and output paths
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    // set mapper and reducer
	    job.setMapperClass(avgLengthMapper.class);
	    job.setReducerClass(avgLengthReducer.class);
	    
	    // set output classes
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    // wait for job to execute
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}