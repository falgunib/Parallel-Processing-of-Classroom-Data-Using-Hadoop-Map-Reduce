/*CSE 587 Project 2 Part 2

 *Q10) How many courses did each department have?
Group Members:  Falguni Bharadwaj - 50163471
		Malavika Tappeta Reddy - 50169248 */

import java.io.IOException;
import java.util.ArrayList;
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

public class Dept10 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text word1 = new Text();
		private Text word2 = new Text();
		private Text num = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split(",");
			//System.out.println(itr.length);
			if(itr.length>8){
				String w = itr[3].trim()+ "/"+ itr[4].trim();
				word.set(w);
				w = "deptc"+"/"+itr[8].trim();
				word1.set(w);
				context.write(word,word1);
				
			}
		}
	}
	public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int max = 0;
			String[] itr = new String[2];
			ArrayList<String> a = new ArrayList<String>();
			for (Text val : values) {
				itr = val.toString().split("/");
				if(itr[0].trim().equals("deptc")){
				if(!(a.contains(itr[1].trim()))){
					a.add(itr[1].trim());
					max += 1;	
				}}
				else
					context.write(key,val);
			}
			//System.out.println("Max: "+max);
			if(itr[0].trim().equals("deptc")){
			result.set(" "+max);
			context.write(key, result);}
	}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Dept10.class);
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