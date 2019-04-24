

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TweetReducer extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable> {
	// Count all occurences of each key
	public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable value : values) sum += value.get();

		// Send to output
		output.write(key, new LongWritable(sum));
	}
}
