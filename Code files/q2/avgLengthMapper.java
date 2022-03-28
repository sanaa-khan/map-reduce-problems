// Sana Ali Khan
// 18i-0439

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class avgLengthMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
	    
		String line = value.toString();
		
		// not all lines contain comments, we know any line with row id has a comment
		if (line.contains("row")) {
	
			// get the row id
	        int rowIndex = line.indexOf("row") + 8;
	        char c = line.charAt(rowIndex);
	        
	        String rowId  = "";
	        
	        while (c != '"') {
	            rowId += c;
	            rowIndex += 1;
	            c = line.charAt(rowIndex);
	        }
	        
	        // get the comment
	        int textIndex = line.indexOf("Text") + 6;
	        c = line.charAt(textIndex);
	        
	        String comment = "";
	        
	        while (c != '"') {
	            comment += c;
	            textIndex += 1;
	            c = line.charAt(textIndex);
	        }
	        
	        // pass on the comment and its length
	        context.write(new Text(rowId), new IntWritable(comment.length()));
		}
		
	}
}
