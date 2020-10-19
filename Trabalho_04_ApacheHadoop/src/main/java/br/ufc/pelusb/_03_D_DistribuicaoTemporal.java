package br.ufc.pelusb;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

public class _03_D_DistribuicaoTemporal {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text words = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String lineTokens = value.toString();
			try {
				JSONObject json = new JSONObject(lineTokens);
				String line = (String) json.get("createdAt");
				String[] tokens = line.split(" ");
				
				if(tokens[2].equals("2015")) {
					words.set(tokens[2] + " " + tokens[0]);
					context.write(words, one);
				} else if(tokens[2].equals("2016")) {
					words.set(tokens[2] + " " + tokens[0]);
					context.write(words, one);
				} else if(tokens[2].equals("2017")) {
					words.set(tokens[2] + " " + tokens[0]);
					context.write(words, one);
				}

			} catch (Exception ex) {
				System.out.println(ex.getMessage());
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(_03_D_DistribuicaoTemporal.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
