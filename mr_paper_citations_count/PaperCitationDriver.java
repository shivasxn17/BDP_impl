
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PaperCitationDriver {

	public static class PaperCitationMapper1 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] tokens = value.toString().trim().split("\t");
			
			System.out.println(tokens);
			String fromNode = tokens[0];

			// ‘toNode’, ‘fromNode’
			context.write(new Text(tokens[1]), new Text(tokens[0]));

			String toNode = String.valueOf(Long.parseLong(tokens[1]) * -1);

			// ‘fromNode’, ‘-toNode’ the "negative" node means that this paper
			// is cited (in the directed graph).
			context.write(new Text(fromNode), new Text(toNode));

		}
	}

	public static class PaperCitationReducer1 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder edge_list = new StringBuilder();

			for (Text val : values) {
				if (String.valueOf(val) != null) {
					edge_list.append(String.valueOf(val));
					edge_list.append(',');
				}
			}

			context.write(key, new Text(edge_list.toString()));
		}
	}

	public static class PaperCitationMapper2 extends Mapper<Object, Text, Text, Text> {

		private Text key_paper = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());

			ArrayList<String> neg_val_list = new ArrayList();
			ArrayList<String> pos_val_list = new ArrayList();

			while (itr.hasMoreTokens()) {
				String neg_key = null;
				String node = itr.nextToken();

				if (node.contains("-")) {
					neg_val_list.add(node.substring(1));
				} else {
					pos_val_list.add(node);
				}


			}
			
			for (int i = 0; i < neg_val_list.size(); i++) {
				key_paper.set(neg_val_list.get(i));
				for (int j = 0; j < pos_val_list.size(); j++) {
					context.write(key_paper, new Text(pos_val_list.get(i)));
				}
			}

		}
	}

	public static class PaperCitationReducer2 extends Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;

			for (Text val : values) {
				sum += 1;
			}
		
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		System.out.println("In PaperCitationDrive............");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PaperCitation at distance 2");

		System.out.println("In Job - Mapper-Reducer 1 Configured............");
		job.setJarByClass(PaperCitationDriver.class);
		job.setMapperClass(PaperCitationMapper1.class);
		job.setCombinerClass(PaperCitationReducer1.class);
		job.setReducerClass(PaperCitationReducer1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "PaperCitation Continued");

		System.out.println("In Job - Mapper-Reducer 2 Configured............");
		job2.setJarByClass(PaperCitationDriver.class);
		job2.setMapperClass(PaperCitationMapper2.class);
		job2.setCombinerClass(PaperCitationReducer2.class);
		job2.setReducerClass(PaperCitationReducer2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);

		// to ensure job2 mapper takes the input as key,value as produced by
		// Reducer
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}w