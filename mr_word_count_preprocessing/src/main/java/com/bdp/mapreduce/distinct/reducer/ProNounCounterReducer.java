
package com.bdp.mapreduce.distinct.reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProNounCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();
	
    enum ReducerCounters {
    SINGULAR, 
    PLURAL
    };
    
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		result.set(sum);
		
		String word = key.toString();
		
		if (word.equalsIgnoreCase("me") || word.equalsIgnoreCase("my") || word.equalsIgnoreCase("mine")
				|| word.equalsIgnoreCase("I")) {
			context.getCounter(ProNounCounterReducer.ReducerCounters.SINGULAR).increment(sum);
		} else if (word.equalsIgnoreCase("us") || word.equalsIgnoreCase("our") || word.equalsIgnoreCase("ours")
				|| word.equalsIgnoreCase("we")) {
			context.getCounter(ProNounCounterReducer.ReducerCounters.PLURAL).increment(sum);
		}
		 
		context.write(key, result);
	}
}