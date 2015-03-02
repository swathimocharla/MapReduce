package anagram;


import java.io.IOException;
import java.util.Arrays;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnagramMapper extends Mapper<Object, Text , Text,  Text>
{
	@Override
	public void map(Object key, Text value, Context context ) 
			throws IOException, InterruptedException
	{
		
		
		String[] words = value.toString().toLowerCase().split("[ \t]+");
		  for(String word:words)
		  {
			  word=word.replaceAll("[^a-z]", "");
		        char[] chars = word.toCharArray();
		        Arrays.sort(chars);
		        String sorted = new String(chars);
			  
			  context.write(new Text(sorted), new Text(word));
		  }
	}
}


