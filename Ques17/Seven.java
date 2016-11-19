/*CSE 587 Project 2 Part 2

 *Q17)Which part of the day is the least busy during exams each semester?
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

public class Seven {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
	{
		private final static IntWritable one = new IntWritable();
		//private Text w0 = new Text();
		private Text word1 = new Text();
		private Text word2 = new Text();
		//private Text num = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] itr = value.toString().split(",");
			if(itr.length==14){
				//word1.set(itr[2].trim());
				String[] w1 = (itr[3].trim()).split(" ");
				if(w1.length==2)
				{
				String word=w1[0].trim()+"_"+w1[1].trim();
				word1.set(word); 
					//String w = w1[0] + "_" + word2;
					//word.set(w);
				String[] time=itr[8].trim().split(":");
				if(time.length==2)
				{
				word2.set(time[0].trim());
			
						context.write(word1,word2);
				}	
				}	
		
	}
}
}


public static class IntSumReducer extends Reducer<Text, Text, Text,Text>
{
	private IntWritable result = new IntWritable();
	
	Text word2 = new Text();
	int mor=0;
	int aft=0;
	int eve=0;
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		
		
		for(Text val : values) {
			//System.out.println("val"+val.toString());
			if((val.toString().contains("Morning"))  ||  (val.toString().contains("Afternoon")) || (val.toString().contains("Evening")))
			{
				context.write(key,val);
				return;
			}
			else{
			String mm=val.toString();
			int v=Integer.parseInt(mm);
			if(v<12)
			{
				mor++;
			}
			else if(v<16)
			{
				aft++;
			}
			else 
			{
				eve++;
			}
			}
			
	
		}
	
		String s=" ";
		//System.out.println(mor+" "+aft+" "+eve);
		if(mor<aft && mor<eve)
			s="Morning";
		else if(aft<mor && aft<eve)
			s="Afternoon";
		else
			s="Evening";
		//result.set(sum);
		word2.set(s);
		
		context.write(key,word2);
		//context.write(word2,result);
	}
}

public static void main(String[] args) throws Exception 
{
	Configuration conf= new Configuration();
	Job job= Job.getInstance(conf, "word count");
	job.setJarByClass(Seven.class);
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
	


	



			
