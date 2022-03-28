// Sana Ali Khan
// 18i-0439

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class classifyKeywordReducer extends Reducer<Text, Text, Text, Text> {
	
	int overallScore = 0;	// overall score for the keyword
	int commentCount = 0;	// comments looked at
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// initialise this comment's score
		int commentScore = 0;
		
		// a value for every word
		for (Text value : values) {
			
			// positive value
		    if (value.toString().equals("1"))
		    	 commentScore += 1;
		      
		    // negative value
		    else if (value.toString().equals("-1"))
		    	  commentScore -= 1;
		}
		
		commentCount += 1;
		
		// writing the comment and its sentiment
		
		String comm = "Comment " + Integer.toString(commentCount) + ": " + key.toString();
		String sentiment = "";
		
		if (commentScore > 0) {
			overallScore += 1;
			
			sentiment = "\nComment Sentiment: Positive as most words are found in the bag of positive words.\n";
		}
		
		else if (commentScore == 0) {
			sentiment = "\nComment Sentiment: Neutral as there are equal numbers of words found in positive/negative bag\n";
		}
		
		else if (commentScore < 0) {
			overallScore -= 1;
			sentiment = "\nComment Sentiment: Negative as most words are found in the bag of negative words.\n";
		}
		
		// writing to the output file
		context.write(new Text(comm), new Text(sentiment));
	}
	
	
	// this method is called when the reducer has finished its work aka time to write final output
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		String output = "\nKeyword: " + classifyKeyword.keyword + " overall usage is ";
		
		if (overallScore > 0)
			output += "positive";
		
		else if (overallScore == 0)
			output += "neutral";
		
		else if (overallScore < 0)
			output += "negative";
		
		context.write(new Text(output), new Text(""));
	}
}