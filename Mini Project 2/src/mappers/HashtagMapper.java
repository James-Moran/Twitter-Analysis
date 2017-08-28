package mappers;

import java.io.IOException;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashtagMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

		// Get value of tweet as string
		String tweet = value.toString();

		// Convert into Json object
		JsonReader tweetDataReader = Json.createReader(new StringReader(tweet.toString()));
		JsonObject tweetDataObject = tweetDataReader.readObject();

		try {
			// Check the language
			if (tweetDataObject.get("lang").toString().replace("\"", "").equals("en")) {

				// Get the entities
				JsonObject entities = tweetDataObject.getJsonObject("entities");

				// Get the hashtags
				JsonArray hashtags = entities.getJsonArray("hashtags");
				for (JsonValue hashtag : hashtags) {
					// Output each tag
					output.write(new Text(((JsonObject) hashtag).get("text").toString().replace("\"", "")),
							new LongWritable(1));
				}
			}
		} catch (NullPointerException e) {
			// No hashtags in tweet
			// Or tweet has been deleted so no language
		}
	}

}
