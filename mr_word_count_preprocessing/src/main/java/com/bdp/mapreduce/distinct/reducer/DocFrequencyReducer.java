
package com.bdp.mapreduce.distinct.reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DocFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();
	enum Counters { AppearedInOneDocument  };
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
        Set<Integer> document = new HashSet();
        
        for( IntWritable val : values ) {
        	document.add(val.get());
        }
        
        if(document.size() == 1) {
            result.set(1);
            context.write(key ,result);
            context.getCounter(DocFrequencyReducer.Counters.AppearedInOneDocument).increment(1);
        }

	}
}