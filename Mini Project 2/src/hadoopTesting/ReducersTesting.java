package hadoopTesting;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mappers.HashtagMapper;
import mappers.SwapMapper;
import reducers.CountReducer;
import reducers.MostPopularReducer;

public class ReducersTesting {

	public static void main(String[] args) throws IOException {

		int maxNumberOfReducers = 10;
		Long[] times = new Long[maxNumberOfReducers];

		for (int i = 1; i < maxNumberOfReducers + 1; i++) {
			long StartTime = System.currentTimeMillis();
			countHashtags(i, Integer.toString(i));
			Order(i, Integer.toString(i));
			long EndTime = System.currentTimeMillis();
			times[i - 1] = (EndTime - StartTime);
		}
		for (int i = 0; i < maxNumberOfReducers; i++) {
			System.out.println(times[i]);
		}

	}

	private static void countHashtags(int numberOfReducers, String fileName) throws IOException {

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

		job.setNumReduceTasks(numberOfReducers);

		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private static void Order(int numberOfReducers, String fileName) throws IOException {

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

		topTweets.setNumReduceTasks(numberOfReducers);

		try {
			topTweets.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
