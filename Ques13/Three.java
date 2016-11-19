/*CSE 587 Project 2 Part 2

 *Q13)Which subject is offered in both spring and fall semester each year?
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

public class Three {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
	{
		//private final static IntWritable one = new IntWritable();
		//private Text w0 = new Text();
		private Text word1 = new Text();
		private Text word2 = new Text();
		//private Text num = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			//System.out.println("value:"+value.toString());
			String[] itr = value.toString().split(",");
			if(itr.length==9){
				//System.out.println("has length 9");
				//word1.set(itr[2].trim());
				String[] w1 = (itr[1].trim()).split(" ");
				if(w1.length==2)
				{
				word1.set(w1[1].trim());
				String word=w1[0].trim()+"-"+(itr[5].trim());
				 word2.set(word);
				 //System.out.println("words set");
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

	Text w0 = new Text();
	Text word2 = new Text();
	Text word3 = new Text();

	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		String s=" ";
		String spring="yes";
		String fall="yes";
		int count=0;
		for(Text val : values) {
			if(val.toString().contains("offered"))
			{
				String[] s1=val.toString().split(":");
				word3.set(s1[1].trim());
				context.write(key,word3);
				return;
			}
			else
			{
			//System.out.println("val is"+val.toString());
			String[] spl=val.toString().split("-");
			if(spl[0].contains("Spring"))
			{
				//System.out.println("contains spring");
				if(fall.contains(spl[1].trim()))
				{
					//System.out.println("similar found in 1");
					s=s.concat(spl[1].trim()+" ");
					//System.out.println("concat done in 1");
					count++;
				}
				spring=spring.concat(spl[1].trim());
			}
			else if(spl[0].contains("Fall"))
			{
				//System.out.println("contains fall");
				if(spring.contains(spl[1].trim()))
				{
					//System.out.println("similar found in 2");
					s=s.concat(spl[1].trim()+" ");
					//System.out.println("concat done in 2");
					count++;
				}
				fall=fall.concat(spl[1].trim());
			}
		}
		}
		//result.set(count);
		//System.out.println("result set");
		w0.set("Subjects offered in both fall and spring in this year are: "+s);
		//String m = Integer.toString(count);
		//word2.set(m);
		context.write(key,w0);
		//System.out.println("write 1 done");
		//context.write(word2,result);
		//System.out.println("write 2 done");
	}
}

public static void main(String[] args) throws Exception 
{
	Configuration conf= new Configuration();
	Job job= Job.getInstance(conf, "word count");
	job.setJarByClass(Three.class);
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
	


	



			
