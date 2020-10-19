package br.ufc.pelusb;
import java.io.IOException;
import java.text.Normalizer;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class _02_E_SentencasRelacionadasAAecio {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] lineTokens = value.toString().split("\t");
				String line = lineTokens[1].trim().replaceAll(" +", " ");
				line = Normalizer.normalize(line, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
				line = line.toLowerCase();
					
				String[] tokens = line.split(" ");
					
				if (tokens.length >= 4) {
					for (int i = 0; i < tokens.length - 3; i++) {
						if (tokens[i].matches("[a-z]+")) {
							if(tokens[i].equals("aecio")) {					
								word.set(tokens[i] + " " + tokens[i + 1] + " " + tokens[i + 2] + " " + tokens[i + 3]);
								context.write(word, one);
							}
						}
					}
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
		Job jobFirst = Job.getInstance(conf, "word count");
		jobFirst.setJarByClass(_02_E_SentencasRelacionadasAAecio.class);
		jobFirst.setMapperClass(TokenizerMapper.class);
		jobFirst.setCombinerClass(IntSumReducer.class);
		jobFirst.setReducerClass(IntSumReducer.class);
		jobFirst.setOutputKeyClass(Text.class);
		jobFirst.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(jobFirst, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobFirst, new Path(args[1]));
		jobFirst.waitForCompletion(true);

		Job jobSecond = new Job(conf, "word count");
		FileInputFormat.setInputPaths(jobSecond, new Path(args[1]));
		FileOutputFormat.setOutputPath(jobSecond, new Path(args[2]));
		jobSecond.setJarByClass(_02_E_SentencasRelacionadasAAecio.class);
		jobSecond.setMapperClass(TrendMapper.class);
		jobSecond.setReducerClass(TrendReducer.class);
		jobSecond.setInputFormatClass(TextInputFormat.class);
		jobSecond.setMapOutputKeyClass(LongWritable.class);
		jobSecond.setMapOutputValueClass(Text.class);
		jobSecond.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		jobSecond.setOutputFormatClass(TextOutputFormat.class);
		System.exit(jobSecond.waitForCompletion(true) ? 1 : 2);

	}
	
	

	public static class TrendMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\t");
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				context.write(new LongWritable(Long.parseLong(tokenizer.nextToken().toString())), new Text(token));

			}
		}

	}
	
	

	public static class TrendReducer extends Reducer<LongWritable, Text, Text, Text> {

		protected void reduce(LongWritable key, Iterable<Text> trends, Context context)
				throws IOException, InterruptedException {

			for (Text val : trends) {
				context.write(new Text(val.toString()), new Text(key.toString()));
			}
		}
	}

}