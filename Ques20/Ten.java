/*CSE 587 Project 2 Part 2

 *Q20)Which building uses the most labs each semester during exams?
Group Members:  Falguni Bharadwaj - 50163471
		Malavika Tappeta Reddy - 50169248 */

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
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

public class Ten {

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
			if(itr.length==13){
				//System.out.println(itr[3].trim());
				//word1.set(itr[2].trim());
				String[] w1 = (itr[3].trim()).split(" ");
				if(w1.length==2)
				{
				String word=w1[0].trim()+"_"+w1[1].trim();
				word1.set(word);
				//System.out.println("word1 set"+word1.toString());
				//System.out.println(itr[5].trim());
				if(itr[5].trim().contains("LAB"))
				{
					
					String[] g=(itr[4].trim()).split(" ");
					word2.set(g[0].trim());
					System.out.println("words: "+word1+"   "+word2);
					context.write(word1,word2);
				}
			}
	}
}
}


public static class IntSumReducer extends Reducer<Text,Text, Text,Text>
{
	private IntWritable result = new IntWritable();
	
	Text word2 = new Text();
	Text word3 = new Text();
	Text em = new Text();
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		HashMap <String,Integer> hm=new HashMap<String,Integer>();
		String s=" ";
		//System.out.println("entered reducer");
		for(Text val : values) {
		if(val.toString().contains("Most"))
		{
			String[] o=val.toString().split("-");
			word3.set(o[1].trim());
			context.write(key,word3);
			return;
		}
		else
		{
		String k=val.toString().trim();	
		if(hm.containsKey(k))
		{
			//System.out.println("contains key");
			hm.put(k, hm.get(k)+1);
		}
		else
		{
			//System.out.println("doesnt contain");
			hm.put(k,1);
		}
		
		}
	}
		//System.out.println("for loop done");
		
		
		int max=Collections.max(hm.values());
		//System.out.println("max: "+max);
		//result.set(max);
		//System.out.println("max set & writing");
		
		//System.out.println("writing done");
		for(Entry<String, Integer> en : hm.entrySet())
		{
			//System.out.println("looking for max");
			if(en.getValue()==max)
			{
				//System.out.println("max found");
				s=s.concat(en.getKey()+" ");
			
				//System.out.println("max key: "+en.getKey());
			}
		}
		s=s.concat(":"+Integer.toString(max));
		Text w0 = new Text();
		w0.set("Most"+"-"+s);
		//System.out.println("w0 set");
		///String m=Integer.toString(max);
		///word2.set(m);
		///em.set(" ");
		//System.out.println("going to write"+w0.toString());
		context.write(key,w0);
		//System.out.println("word2 write done");
		///context.write(w0,em);
		//System.out.println("w0 and null write done");
	}
}

public static void main(String[] args) throws Exception 
{
	Configuration conf= new Configuration();
	Job job= Job.getInstance(conf, "word count");
	job.setJarByClass(Ten.class);
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
	


	



			
