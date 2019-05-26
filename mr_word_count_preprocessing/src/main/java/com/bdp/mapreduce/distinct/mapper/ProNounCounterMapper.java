package com.bdp.mapreduce.distinct.mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import opennlp.tools.stemmer.PorterStemmer;

public class ProNounCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		// removing punctuation from the string
		// Punctuation: One of !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
		String punct_removed = value.toString().replaceAll("\\p{Punct}", "");

		// removing digits from the text- A decimal digit: [0-9]
		String digits_removed = punct_removed.replaceAll("\\p{Digit}", "");

		String[] words = digits_removed.split(" ");

		for (String word : words) {
			if (word.equalsIgnoreCase("me") || word.equalsIgnoreCase("my") || word.equalsIgnoreCase("mine")
					|| word.equalsIgnoreCase("I")) {
				context.write(new Text(word), one);
			} else if (word.equalsIgnoreCase("us") || word.equalsIgnoreCase("our") || word.equalsIgnoreCase("ours")
					|| word.equalsIgnoreCase("we")) {
				context.write(new Text(word), one);
			}
		}
	}
}