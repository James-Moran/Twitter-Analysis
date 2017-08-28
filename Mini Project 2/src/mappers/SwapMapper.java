package mappers;

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SwapMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	public void map(LongWritable key, Text value, Context output) throws IOException {

		String line = value.toString();
		Scanner scanner = new Scanner(line);
		
		String hashtag = scanner.next();
		LongWritable count = new LongWritable(Integer.parseInt(scanner.next()));
		
		
		try {
			output.write(count, new Text(hashtag));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
