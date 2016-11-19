/*CSE 587 Project 2 Part 2

 *Q2) Which course has maximum number of students for each semester.
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

public class MaxSt2 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text word1 = new Text();
		private Text word2 = new Text();
		private Text num = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split(",");
			if(itr.length>8){
				
				String[] w1 = (itr[2].trim()).split(" ");
				word2.set(itr[1].trim());
				String w = itr[6].trim() + "," + itr[7].trim();
					word1.set(w);
					if(itr.length <10) {
						context.write(word2,word1);
				}
		
			}
		}
	}
	public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int max = 0;	
			String[] itr = new String[3];
			String course=" ";
			String c =" ";
			for (Text val : values) {
				itr = val.toString().split(",");
				int m = Integer.parseInt(itr[1].trim());
				System.out.println(itr[0].trim());
				if(m > max){
					max = m;
					course = itr[0].trim();
				}
				if(m == max){
					if(!course.equals(itr[0].trim()))
						course = course +" / "+ itr[0].trim();
				}
				
			}
			
			result.set(max);
			Text value = new Text();
			String v = course + "," + max;
			value.set(v);
			context.write(key, value);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(MaxSt2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}