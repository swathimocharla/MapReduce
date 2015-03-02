package dna1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class DNA1Mapper extends Mapper<Object, Text, Text, Text >
{

	private Text key1 = new Text();
    private Text value1 = new Text(); 
    
    
    @Override
    public void map(Object key, Text value,Context context) 
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] split = line.split(",|\n");
		
		for (int i=0;i<split.length-1;i++)
		{
			
			value1.set(split[i].toString().trim());
			key1.set((split[i+1]).toString().trim()); 
			context.write(key1,value1);
			i++;
            
		}
		
		
	}
    
}
