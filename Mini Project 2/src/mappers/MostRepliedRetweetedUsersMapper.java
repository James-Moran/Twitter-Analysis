package mappers;

import java.io.IOException;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import jdk.nashorn.internal.runtime.UserAccessorProperty;

public class MostRepliedRetweetedUsersMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

		// Get value of tweet as string
		String tweet = value.toString();

		// Convert into Json object
		JsonReader tweetDataReader = Json.createReader(new StringReader(tweet.toString()));
		JsonObject tweetData = tweetDataReader.readObject();

		if (tweetData.getJsonObject("retweeted_status") != null) {
			
			// If retweeted gets the original tweet
			JsonObject originalTweet = tweetData.getJsonObject("retweeted_status");
			
			// Checks the origional tweet is in english
			if (originalTweet.get("lang").toString().replace("\"", "").equals("en")) {
				
				JsonObject originalUser = originalTweet.getJsonObject("user");
				output.write(new Text(originalUser.get("screen_name").toString().replace("\"", "")),
						new LongWritable(1));
			}

		} else if (tweetData.get("in_reply_to_screen_name") != null) {
			// Checks the tweet is in english
			if (tweetData.get("lang").toString().replace("\"", "").equals("en")) {
				// Get the user name of the user being relied to
				String userRepliedName = tweetData.get("in_reply_to_screen_name").toString().replace("\"", "");

				// Output it if it isn't defined as null
				if (!userRepliedName.equals("null")) {
					output.write(new Text(userRepliedName), new LongWritable(1));
				}
			}
		}

	}

}
