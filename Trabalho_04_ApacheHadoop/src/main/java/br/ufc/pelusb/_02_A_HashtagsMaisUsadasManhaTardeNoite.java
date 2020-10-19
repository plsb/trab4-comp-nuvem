package br.ufc.pelusb;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class _02_A_HashtagsMaisUsadasManhaTardeNoite {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private static final int NIGHT = 3;

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] lineTokens = value.toString().split("\t");
				String line = lineTokens[7].trim().replaceAll(" +", " ");	
				String[] tokens = line.split(" ");
				
				String tweetLine = lineTokens[1].trim().replaceAll(" +", " ");		
				tweetLine = tweetLine.toLowerCase();
				
				String stringHour = tokens[3].split(":")[0];
				int hour = Integer.valueOf(stringHour);
				
				Pattern pattern = Pattern.compile("#[a-z0-9]+");
	
				if(NIGHT == 1) {
					if (hour >= 6 && hour < 12) {
						Matcher matcher = pattern.matcher(tweetLine);
						while (matcher.find()) {
							word.set(matcher.group());
							context.write(word, one);
						}
					}
				} else if (NIGHT == 2) {
					if (hour >= 12 && hour < 18) {
						Matcher matcher = pattern.matcher(tweetLine);
						while (matcher.find()) {
							word.set(matcher.group());
							context.write(word, one);
						}
					} 
				} else if(NIGHT == 3) {
					if (hour >= 18 && hour <= 23) {
						Matcher matcher = pattern.matcher(tweetLine);
						while (matcher.find()) {
							word.set(matcher.group());
							context.write(word, one);
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
		jobFirst.setJarByClass(_02_A_HashtagsMaisUsadasManhaTardeNoite.class);
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
		jobSecond.setJarByClass(_02_A_HashtagsMaisUsadasManhaTardeNoite.class);
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