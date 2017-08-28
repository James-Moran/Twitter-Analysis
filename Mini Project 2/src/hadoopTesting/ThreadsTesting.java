package hadoopTesting;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mappers.HashtagMapper;
import mappers.SwapMapper;
import reducers.CountReducer;
import reducers.MostPopularReducer;

public class ThreadsTesting {
	
	public static void main(String[] args) throws IOException {

		int maxNumberOfThreads = 10;
		Long[] times = new Long[maxNumberOfThreads];

		for (int i = 1; i < maxNumberOfThreads + 1; i++) {
			long StartTime = System.currentTimeMillis();
			countHashtags(i, Integer.toString(i));
			Order(i, Integer.toString(i));
			long EndTime = System.currentTimeMillis();
			times[i - 1] = (EndTime - StartTime);
		}
		for (int i = 0; i < maxNumberOfThreads; i++) {
			System.out.println(times[i]);
		}

	}

	private static void countHashtags(int numberOfThreads, String fileName) throws IOException {

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("Hashtags Counting");

		FileInputFormat.setInputPaths(job, new Path("/cs/home/jm361/tmp/Input"));
		FileOutputFormat.setOutputPath(job, new Path("/cs/home/jm361/tmp/Intermediate/" + fileName));

		

		job.setMapperClass(MultithreadedMapper.class);
		
		MultithreadedMapper.setMapperClass(job, HashtagMapper.class);
		
		MultithreadedMapper.setNumberOfThreads(job, numberOfThreads);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

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

	}

	private static void Order(int numberOfThreads, String fileName) throws IOException {

		Configuration conf2 = new Configuration();
		Job topTweets = new Job(conf2);
		topTweets.setJobName("Most Popular");

		FileInputFormat.setInputPaths(topTweets, new Path("/cs/home/jm361/tmp/Intermediate/" + fileName));
		FileOutputFormat.setOutputPath(topTweets, new Path("/cs/home/jm361/tmp/Output/" + fileName));
		
		topTweets.setMapperClass(MultithreadedMapper.class);
		
		MultithreadedMapper.setMapperClass(topTweets, SwapMapper.class);
		
		MultithreadedMapper.setNumberOfThreads(topTweets, numberOfThreads);

		topTweets.setMapOutputKeyClass(LongWritable.class);
		topTweets.setMapOutputValueClass(Text.class);

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
	}

}

