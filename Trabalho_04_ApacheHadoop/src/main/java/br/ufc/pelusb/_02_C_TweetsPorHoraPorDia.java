package br.ufc.pelusb;
import java.io.IOException;

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

import br.ufc.pelusb._02_D_SentencasRelacionadasADilma.TrendMapper;
import br.ufc.pelusb._02_D_SentencasRelacionadasADilma.TrendReducer;

public class _02_C_TweetsPorHoraPorDia {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] lineTokens = value.toString().split("\t");
				String line = lineTokens[8];	
				
				String hourLine = lineTokens[7].trim();
				String[] tokens = hourLine.split(" ");
				
				String stringHour = tokens[3].split(":")[0];
				int hour = Integer.valueOf(stringHour);
				
				for(int i = 0; i < 24; i++) {
					if(hour == i) {
						word.set(hour + " hora(s) do dia " + line);
						context.write(word, one);
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
		jobFirst.setJarByClass(_02_C_TweetsPorHoraPorDia.class);
		jobFirst.setMapperClass(TokenizerMapper.class);
		jobFirst.setCombinerClass(IntSumReducer.class);
		jobFirst.setReducerClass(IntSumReducer.class);
		jobFirst.setOutputKeyClass(Text.class);
		jobFirst.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(jobFirst, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobFirst, new Path(args[1]));
		jobFirst.waitForCompletion(true);
		System.exit(jobFirst.waitForCompletion(true) ? 1 : 2);
		
		Job jobSecond = new Job(conf, "word count");
		FileInputFormat.setInputPaths(jobSecond, new Path(args[1]));
		FileOutputFormat.setOutputPath(jobSecond, new Path(args[2]));
		jobSecond.setJarByClass(_02_C_TweetsPorHoraPorDia.class);
		jobSecond.setMapperClass(TrendMapper.class);
		jobSecond.setReducerClass(TrendReducer.class);
		jobSecond.setInputFormatClass(TextInputFormat.class);
		jobSecond.setMapOutputKeyClass(LongWritable.class);
		jobSecond.setMapOutputValueClass(Text.class);
		jobSecond.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		jobSecond.setOutputFormatClass(TextOutputFormat.class);
		System.exit(jobSecond.waitForCompletion(true) ? 1 : 2);


	}
}