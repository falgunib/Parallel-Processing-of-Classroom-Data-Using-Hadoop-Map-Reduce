/*CSE 587 Project 2 Part 2

 *Q5) Which building has the largest seat utilization?
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

public class LargestUtilization5 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text word1 = new Text();
		private Text word2 = new Text();
		private Text num = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split(",");
			
			if(itr.length>8){
				//System.out.println(itr.length);
				String[] w1 = (itr[2].trim()).split(" ");
				//System.out.println(w1.length);
				if(w1.length == 1 || w1[1].equals("ARR") || w1[1].equals("Arr")) return;
				if(!(w1[0].equals("Arr")) && !(w1[0].equals("Unknown") && w1.length>1)){
					String w = itr[1].trim() + "/" + w1[0].trim();
					word.set(w);
					if(itr.length <10 && !(itr[8].equals(null))) {
				//System.out.println(itr[8]);
						w = w1[1] + "/" + itr[7].trim() + "/" + itr[8].trim();
						word1.set(w);
						context.write(word,word1);
					}
				}
			}
		}
	}
	public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int max1 = 0;
			int max = 0;
			String[] itr = new String[3];
			ArrayList<String> a = new ArrayList<String>();
			for (Text val : values) {
				itr = val.toString().split("/");
				//System.out.println(val);
				//System.out.println(itr[1].trim());
				if(!(a.contains(itr[0].trim()))){
					a.add(itr[0].trim());
					//System.out.println(key);
					//for (String i: a)
						//System.out.println(i);
					int m = Integer.parseInt(itr[1].trim());
					max += m;
					m = Integer.parseInt(itr[2].trim());
					max1 += m; 
				}
				if(itr[0].trim().equals("seats")){
					if(max1!=0){
						int m = max*100/max1;
					
					result.set("seats utilization/"+"in percentage/"+m+"%");
					context.write(key,result);}
				}
				else
					continue;
			}
			//System.out.println("Max: "+max);
			result.set("seats/"+max+"/"+max1);
			context.write(key, result);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(LargestUtilization5.class);
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