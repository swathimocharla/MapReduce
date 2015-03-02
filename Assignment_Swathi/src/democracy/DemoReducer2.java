package democracy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DemoReducer2 extends Reducer<Text, Text, Text, Text>{
	
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException{
		
		
		List<Text> userList = new ArrayList<Text>();
		long sum = 0;
		for(Text it :values) 
		{
            // add the values in the arrayList
			//sum += lw.get();
			
			sum+= Integer.parseInt(it.toString());
			//userList.add(new Text(it.toString()));
            
            }
		//System.out.println(sum);
		//String t= Integer.toString((int) sum);
		context.write(key, new Text(Integer.toString((int) sum)));
		
	}

}