/*CSE 587 Project 2 Part 2

 *Q16)What is the most popular course in each semester?
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

public class Six {

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
				int enroll=Integer.parseInt(itr[7].trim());
				//System.out.println("enroll"+itr[7].trim());
				int total=Integer.parseInt(itr[8].trim());
				//System.out.println("total"+itr[8].trim());
				if(total!=0)
				{
				int percent=(enroll*100)/total;
				String sub=itr[6].trim();
				String send=sub+"-"+Integer.toString(percent);
				word2.set(send);
				//System.out.println("word1 and word2:"+word1+" "+word2);
					//String w = w1[0] + "_" + word2;
					//word.set(w);
			
						context.write(word1,word2);
				}
					
				
		
	}
}
}


public static class IntSumReducer extends Reducer<Text, Text, Text,Text>
{
	private IntWritable result = new IntWritable();
	
	Text word2 = new Text();
	Text word3 = new Text();
	
	//String subject="s";
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		int max=0;
		String sub=" ";
		for(Text val : values) {
			//System.out.println("key: "+key.toString()+"\nval: "+val.toString());
			if(val.toString().contains("Most"))
			{
				String[] s1=val.toString().split(":");
				word3.set(s1[1].trim());
				context.write(key,word3);
				return;
			}
			else
			{
			String[] spl=val.toString().split("-");
			if(spl.length==2)
			{
			int percent=Integer.parseInt(spl[1].trim());
			if(percent>max)
			{
				System.out.println("percent>max");
				//subject="The most popular subjects for this semester are: ";
				max=percent;
				sub=spl[0].trim();
				//subject=subject.concat(spl[0].trim()+",");
				//System.out.println("sub:"+sub);
				
			}
			}
			word2.set("Most popular course is: "+sub);
		}
		}
		//subject="The most popular subject for this semester is: "+sub;
		//System.out.println("sub after concat"+sub);
		//result.set(sum);
		
		//System.out.println("final word"+word2);
		context.write(key,word2);
		//System.out.println("write done");
		//context.write(word2,result);
	}
}

public static void main(String[] args) throws Exception 
{
	Configuration conf= new Configuration();
	Job job= Job.getInstance(conf, "word count");
	job.setJarByClass(Six.class);
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
	


	



			
