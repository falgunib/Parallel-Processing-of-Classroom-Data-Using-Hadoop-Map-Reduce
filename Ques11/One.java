/*CSE 587 Project 2 Part 2

 *Q11)What are the total number of courses offered each semester?
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

public class One {

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
				String l=itr[5].trim();
				word2.set(l);
					//String w = w1[0] + "_" + word2;
					//word.set(w);
					//System.out.println("in mapper and writing");
						context.write(word1,word2);
						//System.out.println("mapper writing done");
				
		
	}
}
}


public static class IntSumReducer extends Reducer<Text, Text, Text,Text>
{
	private IntWritable result = new IntWritable();
	
	private Text word2 = new Text();
	private Text word3 = new Text();
	Text w = new Text();
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		int sum=0;
		String s=" ";
		for(Text val : values) {
			if(val.toString().contains("Total"))
			{
				String[] s1=val.toString().split(":");
				word3.set(s1[1].trim());
				context.write(key,word3);
				return;
			}
			else
			{
			String check=val.toString();
			//if(key.toString().contains("Winter_2017"))
				//System.out.println("winter");
			if(s.contains(check))
				break;
			else
			{
			s=s.concat(check);
			sum=sum+1;
			}
			//else 
			}//break;
		}
		//System.out.println("sum"+sum);
		String h="Total number of courses: "+Integer.toString(sum);
		
		w.set(h);
		//result.set(sum);
		//word2.set(s);
		//System.out.println("in reducer and writing");
		context.write(key,w);
		//System.out.println("reducer writing done");
		//context.write(word2,result);
	}
}

public static void main(String[] args) throws Exception 
{
	Configuration conf= new Configuration();
	Job job= Job.getInstance(conf, "word count");
	job.setJarByClass(One.class);
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
	


	



			
