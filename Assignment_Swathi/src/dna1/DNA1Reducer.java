package dna1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DNA1Reducer extends Reducer<Text, Text, Text, Text>{
	
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException{
		
		
		List<String> userList = new ArrayList<String>();
		
		for(Text it :values) 
		{
            // add the values in the arrayList
			userList.add(it.toString());
            
            }

		context.write(key,  new Text(userList.toString()));
		
	}

}
