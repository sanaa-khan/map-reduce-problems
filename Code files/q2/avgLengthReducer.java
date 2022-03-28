// Sana Ali Khan
// 18i-0439

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class avgLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	int lengthSum = 0;
	int lengthCount = 0;
	
	@Override
	  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		// add the average length of the comment
		for (IntWritable value : values) {
		      lengthSum += value.get();
		      lengthCount += 1;
		}
	}
	
	// this method is called when the reducer has finished its work aka time to write final output
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		int avg = lengthSum / lengthCount;
		String text = "Average length of a comment: ";
    	context.write(new Text(text), new IntWritable(avg));
	}
}