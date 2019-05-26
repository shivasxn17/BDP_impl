package com.bdp.mapreduce.distinct.mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import opennlp.tools.stemmer.PorterStemmer;


public class TextPreprocesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		 
		// removing punctuation from the string 
		// Punctuation: One of !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
		String punct_removed = value.toString().replaceAll("\\p{Punct}", "");
		
		// removing digits from the text-  A decimal digit: [0-9]
		String digits_removed = punct_removed.replaceAll("\\p{Digit}", "");
		
		// removing alphanumerics from the text- An alphanumeric character:[\p{Alpha}\p{Digit}] 
		// alphabet and decimal digit: [0-9] together
		// String preprocessed_txt = digits_removed.replaceAll("\\p{Alnum}", "");
		
		// to stem 's 'ing and other form of same word, bringing it root word
		PorterStemmer porterStemmer = new PorterStemmer();

		// normalizing by converting all letters to lowercase
		String[] words = digits_removed.toLowerCase().split(" ");
		
		for (String word : words) {
			String stemmed_word = porterStemmer.stem(word);
			context.write(new Text(stemmed_word), one);
		}
	}
}