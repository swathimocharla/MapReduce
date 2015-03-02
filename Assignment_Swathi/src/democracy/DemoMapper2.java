package democracy;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DemoMapper2 extends Mapper<Text, Text, Text, Text >
{

	private Text key2 = new Text();
    private Text value2 = new Text();
    
    @Override
    public void map(Text key, Text value,Context context) 
			throws IOException, InterruptedException {
		
		
		String line = value.toString().replaceAll( "[^a-zA-Z0-9,\n]", "");
		String[] split = line.split(",|\n");
		if (split.length>1)
		{
			
		for (int i=1;i<split.length;i++)
		{
			
			value2.set(split[0].toString().trim());
			key2.set(split[i].toString().trim());
			
			
			//System.out.println(key2.toString()+ " - "+value2.toString());
			//System.out.println(key2.toString());
			context.write(key2,value2);
            
		}
		}
	}
}