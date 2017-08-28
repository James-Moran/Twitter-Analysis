package mappers;

import java.io.IOException;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MostRepliedTweetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

		// Get value of tweet as string
		String tweet = value.toString();

		// Convert into Json object
		JsonReader tweetDataReader = Json.createReader(new StringReader(tweet.toString()));
		JsonObject tweetDataObject = tweetDataReader.readObject();

		try {
			// Checks the tweets language is English
			if (tweetDataObject.get("lang").toString().replace("\"", "").equals("en")) {

				// Checks the values isn't set to null
				if (!tweetDataObject.get("in_reply_to_status_id").toString().replace("\"", "").equals("null")) {

					output.write(new Text(tweetDataObject.get("in_reply_to_status_id").toString().replace("\"", "")),
							new LongWritable(1));
				}
			}
		} catch (NullPointerException e) {
			// Not a reply or has no language specified
		}
	}
}
