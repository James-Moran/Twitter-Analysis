package twitterAnalysis;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Date;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mappers.HashtagMapper;
import mappers.MostRepliedRetweetedUsersMapper;
import mappers.MostRepliedTweetMapper;
import mappers.MostRetweetedTweetMapper;
import mappers.SwapMapper;
import reducers.CountReducer;
import reducers.MostPopularReducer;
import sun.launcher.resources.launcher;

public class TwitterMostPopular {

	public static void main(String[] args) throws IOException {
		countHashtags();
//		mostRetweetedTweets();
//		mostRepliedTweets();
//		mostRepliedandRetweetedUsers();

	}

	private static void countHashtags() throws IOException {
		
		String fileName = "Hashtags";
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("Hashtags Counting");

		FileInputFormat.setInputPaths(job, new Path("/cs/home/jm361/tmp/Input"));
		FileOutputFormat.setOutputPath(job, new Path("/cs/home/jm361/tmp/Intermediate/" + fileName));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setMapperClass(HashtagMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setReducerClass(CountReducer.class);

		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		Order(fileName);

	}
	
	private static void mostRetweetedTweets() throws IOException {
		
		String fileName = "RetweetedTweets";

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("Most Retweeted Tweets");

		FileInputFormat.setInputPaths(job, new Path("/cs/home/jm361/tmp/Input"));
		FileOutputFormat.setOutputPath(job, new Path("/cs/home/jm361/tmp/Intermediate/" + fileName));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setMapperClass(MostRetweetedTweetMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setReducerClass(CountReducer.class);

		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		Order(fileName);

	}
	
	private static void mostRepliedTweets() throws IOException {
		
		String fileName = "RepliedTweets";

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("Most Replied Tweets");

		FileInputFormat.setInputPaths(job, new Path("/cs/home/jm361/tmp/Input"));
		FileOutputFormat.setOutputPath(job, new Path("/cs/home/jm361/tmp/Intermediate/" + fileName));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setMapperClass(MostRepliedTweetMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setReducerClass(CountReducer.class);

		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		Order(fileName);

	}
	
	private static void mostRepliedandRetweetedUsers() throws IOException {
		
		String fileName = "RepliedRetweetedUsers";

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("Most Replied Users");

		FileInputFormat.setInputPaths(job, new Path("/cs/home/jm361/tmp/Input"));
		FileOutputFormat.setOutputPath(job, new Path("/cs/home/jm361/tmp/Intermediate/" + fileName));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setMapperClass(MostRepliedRetweetedUsersMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setReducerClass(CountReducer.class);

		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		Order(fileName);

	}

	private static void Order(String fileName) throws IOException {

		Configuration conf2 = new Configuration();
		Job topTweets = new Job(conf2);
		topTweets.setJobName("Most Popular");

		FileInputFormat.setInputPaths(topTweets, new Path("/cs/home/jm361/tmp/Intermediate/" + fileName));
		FileOutputFormat.setOutputPath(topTweets, new Path("/cs/home/jm361/tmp/Output/" + fileName));

		topTweets.setMapOutputKeyClass(LongWritable.class);
		topTweets.setMapOutputValueClass(Text.class);

		topTweets.setMapperClass(SwapMapper.class);

		topTweets.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		topTweets.setOutputKeyClass(LongWritable.class);
		topTweets.setOutputValueClass(Text.class);
		

		topTweets.setReducerClass(MostPopularReducer.class);

		try {
			topTweets.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		getTop40(fileName);

	}
	
	private static void getTop40(String fileName) throws IOException {
		int lines = 0;
		
		Scanner outputReader = new Scanner(new File("/cs/home/jm361/tmp/Output/" + fileName + "/part-r-00000"));
		PrintWriter top40writer = new PrintWriter("/cs/home/jm361/tmp/Output/" + fileName + "/top40");
		
		while(lines < 40 && outputReader.hasNextLine()){		
			top40writer.println(outputReader.nextLine());
			lines++;
		}
		
		outputReader.close();
		top40writer.close();	
	}
}
