package cdle.wordcount.mr;

import java.io.IOException;

import cdle.wordcount.lib.WordCountUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer
			extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable result = new IntWritable();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		
		result.set(sum);
		context.write(key, result);

		// count the distincts
		context.getCounter(WordCountUtils.Statistics.Distincts).increment(1);

		// count the singletons
		if ( sum == 1 )
			context.getCounter(WordCountUtils.Statistics.Singletons).increment(1);

	}

}




