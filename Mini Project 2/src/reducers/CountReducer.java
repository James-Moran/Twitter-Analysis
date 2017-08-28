package reducers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException {

		// The key is the word.
		// The values are all the counts associated with that word (commonly one copy of '1' for each occurrence).

		int sum = 0;
		for(LongWritable value : values){
			long l = value.get();
			sum += l;
		}
		try {
			output.write(key, new LongWritable(sum));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
