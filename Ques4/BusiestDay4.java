/*CSE 587 Project 2 Part 2

 *Q4) Which day was busiest each semester?
Group Members:  Falguni Bharadwaj - 50163471
		Malavika Tappeta Reddy - 50169248 */

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BusiestDay4 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text word1 = new Text();
		private Text word2 = new Text();
		private Text num = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split(",");
			if(itr.length>8){
				if(!(itr[3].trim().equals("UNKWN")) && !(itr[3].trim().equals("ARR"))){ 
					String[] days = itr[3].trim().split("");
					for(String d : days){
					
						if(d.equals("-") || d.equals(" ") || d.equals(null)) return;
						String w =itr[1].trim() + "/" + d.trim() ;
						word.set(w);
				
						if(itr.length <10) {
							context.write(word,one);
						}
					}
				}
			}
		}
	}
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
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
		job.setJarByClass(BusiestDay4.class);
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