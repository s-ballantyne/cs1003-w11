

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.*;
import java.io.StringReader;
import java.util.Scanner;

public class TweetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	// value (Text) is the tweet
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
		// Parse tweet as JSON
		JsonReader reader = Json.createReader(new StringReader(value.toString()));
		JsonObject user = reader.readObject().getJsonObject("user");

		// If the tweet contains a non-null screen name and non-null location
		if (user != null
				&& !user.isNull("screen_name")
				&& !user.isNull("location"))
		{
			String name = user.getString("screen_name"),
					location = user.getString("location");

			// Unquote name & location if possible
			if (name.charAt(0) == '"'
					&& name.charAt(name.length() - 1) == '"')
				name = name.substring(1, name.length() - 1);

			if (location.charAt(0) == '"'
					&& location.charAt(location.length() - 1) == '"')
				location = location.substring(1, location.length() - 1);

			// Format results
			String res = name + " (" + location + ")";

			// Remove newlines and carriage returns
			res = res.replace("\n", " ");
			res = res.replace("\r", " ");

			// Fix ridiculous bug
			// Uncomment this line and the last test will fail
			// due to some stupid Arabic character
			res = res.replaceAll("\u0016", " \u0016");

			// Send to output
			output.write(new Text(res), new LongWritable(1));
		}
	}
}
