// Sana Ali Khan
// 18i-0439

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class classifyKeywordMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    
		String line = value.toString();
		
		if (line.contains("row")) {
			
			// starting index of comment in the line
	        int textIndex = line.indexOf("Text") + 6;
	        char c = line.charAt(textIndex);
	        
	        String comment = "";
	        
	        // get the whole commend
	        while (c != '"') {
	            comment += c;
	            textIndex += 1;
	            c = line.charAt(textIndex);
	        }
	        
	        // if comment has our specified keyword
			if (comment.contains(classifyKeyword.keyword)) {
				
				// get all the words in the comment
				String[] words = comment.split(" ");
				
				// for each word
				for (String word: words) {
					// if word is positive
					if (classifyKeyword.positiveWords.contains(word)) {
						context.write(new Text(comment), new Text("1"));
					}
					
					// if word is negative
					else if (classifyKeyword.negativeWords.contains(word)) {
						context.write(new Text(comment), new Text("-1"));
					}
					
					else
						continue;
				}
			}
		}
	}
}