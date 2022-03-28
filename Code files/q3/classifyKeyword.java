// Sana Ali Khan
// 18i-0439

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class classifyKeyword {
	
	// static or 'global' variables
	static String keyword = "";
	
	static ArrayList<String> positiveWords;
	static ArrayList<String> negativeWords;
	
	// this method will get all positive/negative words and store them into their respective lists
	public static void load_data() throws FileNotFoundException {
		
		// open the word files (I have given absolute path of the files on my system)
		File positiveFile = new File("/home/sana/hadoopMR/extra_input/positive.txt");
		File negativeFile = new File("/home/sana/hadoopMR/extra_input/negative.txt");
		
		// allocate memory
		positiveWords = new ArrayList<String>();
		negativeWords = new ArrayList<String>();
		
		// reading file for positive words
		Scanner reader = new Scanner(positiveFile);
		
		// reading each word and adding to list
		while (reader.hasNextLine()) {
			String word = reader.nextLine();
			positiveWords.add(word);
		}
		
		// closing the reader
		reader.close();
		
		// reading file for negative words
		reader = new Scanner(negativeFile);
		
		// reading each word and adding to list
		while (reader.hasNextLine()) {
			String word = reader.nextLine();
			negativeWords.add(word);
		}
		
		// closing the reader
		reader.close();
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 3) {
		      System.err.println("Usage: classifyKeyword <input path> <output path> <keyword>");
		      System.exit(-1);
		}
		
		// get keyword from argument
		keyword = args[2];
		// load data from files
		load_data();
		    
		// configuration and job
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Classify Keyword");
	    
	    // set job properties
	    job.setJarByClass(classifyKeyword.class);
	    job.setJobName("ClassifyKeyword");
	    
	    // set input and output
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    // set mapper and reducer
	    job.setMapperClass(classifyKeywordMapper.class);
	    job.setReducerClass(classifyKeywordReducer.class);
	    
	    // set output classes 
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    // wait for job to execute
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}