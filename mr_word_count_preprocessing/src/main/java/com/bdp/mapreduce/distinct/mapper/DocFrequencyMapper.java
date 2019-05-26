
package com.bdp.mapreduce.distinct.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DocFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private static int num_document = 0;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		num_document++;

		// removing punctuation from the string
		// Punctuation: One of !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
		String punct_removed = value.toString().replaceAll("\\p{Punct}", "");

		// removing digits from the text- A decimal digit: [0-9]
		String digits_removed = punct_removed.replaceAll("\\p{Digit}", "");

		StringTokenizer itr = new StringTokenizer(digits_removed);
		
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, new IntWritable(num_document));
		}
		
	}
}