package letterfreq;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class LFMapper extends Mapper<Object, Text , IntWritable, LongWritable>
{
	@Override
	public void map(Object key, Text value, Context context ) 
			throws IOException, InterruptedException
	{
		String newtext = value.toString().replaceAll( "[^a-zA-Z|\\t]", "").toLowerCase();
		for(int i=0; i < newtext.length(); i++)
		  {
			  char ch = newtext.charAt(i);
			  context.write(new IntWritable(ch), new LongWritable(1));
		  }
		
		
		/*String line = value.toString();
		  
		  for(int i=0; i < line.length(); i++)
		  {
			  char ch = line.charAt(i);
			 // System.out.println(ch);
			  context.write(new IntWritable(ch), new LongWritable(1));
		  }*/
	}

}
