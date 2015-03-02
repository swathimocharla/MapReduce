package letterfreq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import javax.naming.ldap.PagedResultsControl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.fail;

public class LFTest {


	MapDriver<Object, Text , IntWritable, LongWritable> mapDriver;
	ReduceDriver<IntWritable, LongWritable, Text, LongWritable> reduceDriver;
	MapReduceDriver<Object, Text, IntWritable, LongWritable, Text, LongWritable> mapReduceDriver;

	
	@Before
	public void setUp() 
	{

		/*
		 * Set up the mapper test harness.
		 */
		LFMapper mapper = new LFMapper();
		mapDriver = new MapDriver<Object, Text, IntWritable, LongWritable>();
		mapDriver.setMapper(mapper);
		
		
		LFReducer reducer = new LFReducer();
		reduceDriver = new ReduceDriver<IntWritable, LongWritable, Text, LongWritable>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<Object, Text, IntWritable, LongWritable, Text, LongWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);	
		
	}
	
	
	//@Test
	public void testMap() throws IOException {

		File file = new File("D:\\new.txt");
	    StringBuffer contents = new StringBuffer();
	    BufferedReader reader = null;

	    reader = new BufferedReader(new FileReader(file));
	    String text = null;

	    // repeat until all lines is read
	    while ((text = reader.readLine()) != null) {
	      contents.append(text).append(System.getProperty("line.separator"));
	    }
	    reader.close();
	    //System.out.println(contents.toString());
	    
	    mapDriver.setInput(new Pair<Object, Text>("1", new Text(contents.toString())));
	    
	    List<Pair<IntWritable, LongWritable>> output = mapDriver.run();
		
		for (Pair<IntWritable, LongWritable> p : output) {
			System.out.println(p.getFirst() + " - " + p.getSecond());
		}
		
	}
	
	@Test
	public void testMapReduce() throws IOException {

		
		File file = new File("D:\\new.txt");
	    StringBuffer contents = new StringBuffer();
	    BufferedReader reader = null;

	    reader = new BufferedReader(new FileReader(file));
	    String text = null;

	    // repeat until all lines is read
	    while ((text = reader.readLine()) != null) {
	      contents.append(text).append(System.getProperty("line.separator"));
	    }
	    reader.close();
	    //System.out.println(contents.toString());
	    
	    mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text(contents.toString())));
	    
	    List<Pair<Text, LongWritable>> output = mapReduceDriver.run();
		
		for (Pair<Text, LongWritable> p : output) {
			System.out.println(p.getFirst() + " - " + p.getSecond());
			
			
		}
	}
}
