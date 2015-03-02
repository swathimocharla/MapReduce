package democracy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DemoReducer1 extends Reducer<Text, Text, Text, Text>{
	
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException{
		
		
		List<Text> userList = new ArrayList<Text>();
		
		for(Text it :values) 
		{
            // add the values in the arrayList
			userList.add(new Text(it.toString()));
            
            }
		context.write(key, new Text(userList.toString()));
		
	}

}
