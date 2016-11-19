/*CSE 587 Project 2 Part 2

 *Q18) Each year between spring and fall which semester has higher enrollment?
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

public class Eight {

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
			if(itr.length==9){
				//word1.set(itr[2].trim());
				String[] w1 = (itr[1].trim()).split(" ");
				if(w1.length==2 && ((Integer.parseInt(itr[7].trim())>=0)))
				{
				String word=w1[0].trim()+"-"+itr[7].trim();
				word1.set(w1[1].trim()); 
					//String w = w1[0] + "_" + word2;
					//word.set(w);
				//String[] time=itr[8].trim().split(":");
			
				word2.set(word);
				//System.out.println(word2);
						 
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
	//int eve=0;
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		int spring=0;
		int fall=0;
		
		for(Text val : values) {
			//System.out.println("val"+val.toString());
			if((val.toString().contains("Enrollment")))
			{
				String[] s=val.toString().split("/");
				word3.set(s[1].trim());
				context.write(key,word3);
				return;
			}
			else{
				
			String[] spl=val.toString().split("-");
			if(!(spl[1].trim().equals(""))||!(spl[1].trim().equals(null)))
			{
			if(spl[0].contains("Spring"))
			{
				int i=Integer.parseInt(spl[1].trim());
				spring=spring+i;
				//System.out.println("Spring: "+spring);
			}
			else if(spl[0].contains("Fall"))
			{
				int i=Integer.parseInt(spl[1].trim());
				fall=fall+i;
			}
			}
	
		}
		}
	
		String s=" ";
		//System.out.println(mor+" "+aft+" "+eve);
		if(spring>fall)
			s="Enrollment"+"/"+"Spring"+"-"+Integer.toString(spring);
		else if(fall>spring)
			s="Enrollment"+"/"+"Fall"+"-"+Integer.toString(fall);
		else if(fall==spring)
			s="Enrollment"+"/"+"Fall & Spring"+"-"+Integer.toString(fall);
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
	job.setJarByClass(Eight.class);
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
	


	



			
