package mappers;

import java.io.IOException;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MostRetweetedTweetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

		// Get value of tweet as string
		String tweet = value.toString();

		// Convert into Json object
		JsonReader tweetDataReader = Json.createReader(new StringReader(tweet.toString()));
		JsonObject tweetDataObject = tweetDataReader.readObject();

		try {
			
			// Gets the original tweet
			JsonObject originalTweet = tweetDataObject.getJsonObject("retweeted_status");
			
			// Checks the orignal tweets language is english
			if (originalTweet.get("lang").toString().replace("\"", "").equals("en")) {
				
				output.write(new Text(originalTweet.get("id_str").toString().replace("\"", "")), new LongWritable(1));
			}

		} catch (NullPointerException e) {
			// Not a retweet or has no language specified
		}

	}
}
