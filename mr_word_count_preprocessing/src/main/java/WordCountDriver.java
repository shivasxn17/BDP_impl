import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.bdp.mapreduce.distinct.mapper.WordCountMapper;
import com.bdp.mapreduce.distinct.mapper.DocFrequencyMapper;
import com.bdp.mapreduce.distinct.mapper.NoStopWordsMapper;
import com.bdp.mapreduce.distinct.mapper.PoliticalConceptMapper;
import com.bdp.mapreduce.distinct.mapper.ProNounCounterMapper;
import com.bdp.mapreduce.distinct.mapper.TextPreprocesMapper;
import com.bdp.mapreduce.distinct.reducer.WordCountReducer;
import com.bdp.mapreduce.distinct.reducer.LessThanFourReducer;
import com.bdp.mapreduce.distinct.reducer.ProNounCounterReducer;
import com.bdp.mapreduce.distinct.reducer.DocFrequencyReducer;;

public class WordCountDriver extends Configured implements Tool {
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		// Configured conf = new Configured();

		int task = Integer.parseInt(args[0]);
		Job job = new Job(getConf(), "Word Count Job");
		
		if (task == 1) {
			
			job.setJobName("Word Count in Corpus");
			job.setJarByClass(getClass());
			
			// task 1 mapper for term count
			job.setMapperClass(WordCountMapper.class);

			// task 1 reducer for unique terms
			job.setReducerClass(WordCountReducer.class);

		} else if (task == 2) {
			
			job.setJobName("Text Preprocessing");
			job.setJarByClass(getClass());
			
			// task 2 mapper for text precessing and unique terms
			job.setMapperClass(TextPreprocesMapper.class);
			job.setReducerClass(WordCountReducer.class);
			
		} else if (task == 3) {
			
			job.setJobName("Terms Occured less than 4 times");
			job.setJarByClass(getClass());
			
			job.setMapperClass(TextPreprocesMapper.class);
			// task 3 reducer for less than 4 occurrences check
			job.setReducerClass(LessThanFourReducer.class);
			
//			job.waitForCompletion(true);
//			
//		    Counters counters = job.getCounters();
//		    Counter ctr = (Counter) counters.findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);
//		    System.out.print("No of Terms Occured less than 4 times:  " + ctr.getValue());
//		    

		} else if (task == 4) {
			
			job.setJobName("ProNouns Counter");
			job.setJarByClass(getClass());
			
			job.setMapperClass(ProNounCounterMapper.class);
			job.setReducerClass(ProNounCounterReducer.class);
			
			//job.waitForCompletion(true);
		    //Counters counters = job.getCounters();
		    //Counter ctr = counters.findCounter(ProNounCounterReducer.ReducerCounters.SINGULAR);
		    //System.out.print("No of Terms Occured less than 4 times:  " + ctr.getValue());
			
		} else if (task == 5) {
			
			job.setJobName("Terms Appeared in Only One Doc Counter");
			job.setJarByClass(getClass());
			
			job.setMapperClass(DocFrequencyMapper.class);
			job.setReducerClass(DocFrequencyReducer.class);
			

		} else if (task == 6) {
			
			job.setJobName("Removing Stop Words from Corpus");
			job.setJarByClass(getClass());
			
			job.setMapperClass(NoStopWordsMapper.class);
			job.setReducerClass(WordCountReducer.class);

		} else if (task == 7) {
			
			job.setJobName("Political Concept 5 words after it");
			job.setJarByClass(getClass());
			
			job.setMapperClass(PoliticalConceptMapper.class);
			job.setReducerClass(WordCountReducer.class);
			
		} else {
			System.out.println("Please provide task number to be executed as first argument/parameter");
		}

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int jobStatus = ToolRunner.run(new WordCountDriver(), args);
		System.exit(jobStatus);
	}
}
