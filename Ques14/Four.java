/*CSE 587 Project 2 Part 2

 *Q14)What is the total enrollment in each semester?
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Four {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
	{
		//private final static IntWritable one = new IntWritable();
		//private Text w0 = new Text();
		private Text word1 = new Text();
		private Text word2 = new Text();
		//private Text num = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] itr = value.toString().split(",");
			if(itr.length==9){
				//word1.set(itr[2].trim());
				String[] w1 = (itr[1].trim()).split(" ");
				String word=w1[0].trim()+"_"+w1[1].trim();
				word1.set(word); 
				String en=itr[7].trim();
				word2.set(en);
					//String w = w1[0] + "_" + word2;
					//word.set(w);
			
						context.write(word1,word2);
					
				
		
	}
}
}


public static class IntSumReducer extends Reducer<Text, Text, Text,Text>
{
	//private IntWritable result = new IntWritable();
	//String s="s";
	 Text word2 = new Text();
	 Text word3 = new Text();
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		int sum=0;
		for(Text val : values) {
			if(val.toString().contains("Total"))
			{
				String[] s=val.toString().split(":");
				word3.set(s[1].trim());
				context.write(key,word3);
				return;
			}
			else
			{
			String c=val.toString();
			int d=Integer.parseInt(c);
			sum=sum+d;
			}
		}
		//result.set(sum);
		word2.set("Total enrollment: "+Integer.toString(sum));
		context.write(key,word2);
		//context.write(word2,result);
	}
}

public static void main(String[] args) throws Exception 
{
	Configuration conf= new Configuration();
	Job job= Job.getInstance(conf, "word count");
	job.setJarByClass(Four.class);
	job.setMapperClass(TokenizerMapper.class);
	job.setCombinerClass(IntSumReducer.class);
	job.setReducerClass(IntSumReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0: 1);
	
}

}
	


	



			
