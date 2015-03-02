package anagram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnagramReducer extends Reducer<Text, Text, Text, Text>{
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException,InterruptedException
	{ 
		
		/*ArrayList<Text> list = new ArrayList<Text>();    
    for (Text val : values) {
        list.add(new Text(val));
        
     }
    System.out.println("here");
    System.out.println(list);
    //context.write(key, new ArrayWritable(IntWritable.class, list.toArray(new IntWritable[list.size()])));
    context.write(key, new ArrayWritable(Text.class, list.toArray(new Text[list.size()])));
    //context.write(key, new Text());
*/    
    
    List<String> anaList = new ArrayList<String>();
	
	for(Text it :values) 
	{
        // add the values in the arrayList
		anaList.add(it.toString());
        
        }
	context.write(key,  new Text(anaList.toString()));
    }

}
