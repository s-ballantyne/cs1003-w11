

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.*;
import java.io.StringReader;
import java.util.Scanner;

public class LocationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	// value (Text) is the tweet
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
		// Parse tweet as JSON
		JsonReader reader = Json.createReader(new StringReader(value.toString()));
		JsonObject user = reader.readObject().getJsonObject("user");

		// If the tweet contains a non-null location
		if (user != null
				&& !user.isNull("location"))
		{
			String location = user.getString("location");

			// Unquote name & location if possible
			if (location.charAt(0) == '"'
					&& location.charAt(location.length() - 1) == '"')
				location = location.substring(1, location.length() - 1);

			location = location.replaceAll("\n", " ");
			location = location.replaceAll("\r", " ");

			// Send to output
			output.write(new Text(location), new LongWritable(1));
		}
	}
}
