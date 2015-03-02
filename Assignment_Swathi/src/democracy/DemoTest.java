package democracy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class DemoTest {

	MapDriver<Object, Text, Text, Text> mapDriver1;
	MapDriver<Text, Text, Text, Text> mapDriver2;
	ReduceDriver<Text, Text, Text, Text> reduceDriver1;
	ReduceDriver<Text, Text, Text, Text> reduceDriver2;
	MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver1;
	MapReduceDriver<Text, Text, Text, Text, Text, Text> mapReduceDriver2;

	
	@Before
	public void setUp() 
	{

		/*
		 * Set up the mapper test harness.
		 */
		DemoMapper1 mapper1 = new DemoMapper1();
		mapDriver1 = new MapDriver<Object, Text, Text, Text>();
		mapDriver1.setMapper(mapper1);
		
		Mapper<Text, Text, Text, Text> mapper2 = new DemoMapper2();
		mapDriver2 = new MapDriver<Text, Text, Text, Text>();
		mapDriver2.setMapper(mapper2);
		
		DemoReducer1 reducer1 = new DemoReducer1();
		reduceDriver1 = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver1.setReducer(reducer1);
		
		DemoReducer2 reducer2 = new DemoReducer2();
		reduceDriver2 = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver2.setReducer(reducer2);
		
		mapReduceDriver1 = new MapReduceDriver<Object, Text, Text, Text, Text, Text>();
		mapReduceDriver1.setMapper(mapper1);
		mapReduceDriver1.setReducer(reducer1);	
		
		mapReduceDriver2 = new MapReduceDriver<Text, Text, Text, Text, Text, Text>();
		mapReduceDriver2.setMapper(mapper2);
		mapReduceDriver2.setReducer(reducer2);	
		
	}
	
	
	//@Test
	public void testMap() throws IOException {

		String[] bothstr= {"D:\\Demo1.csv","D:\\Demo2.csv"};
		List<Pair<Text, Text>> output = null;
		StringBuffer contents = new StringBuffer();
		for (int i=0; i<bothstr.length;i++)
		{
			File csvFile  = new File(bothstr[i].toString().trim());
		    
		    BufferedReader reader = null;

		    reader = new BufferedReader(new FileReader(csvFile));
		    String text = null;

		    // repeat until all lines is read
		    while ((text = reader.readLine()) != null) {
		      contents.append(text).append(System.getProperty("line.separator"));
		    }
		    reader.close();
		}   
		    mapDriver1.setInput(new Pair<Object, Text>("1", new Text(contents.toString())));
		    
		    output = mapDriver1.run();
			
			for (Pair<Text, Text> p : output) {
				System.out.println(p.getFirst() + " - " + p.getSecond());
		}
			System.out.println(output.size());
	}
	
	@Test
	public void testMapReduce() throws IOException {

		String[] bothstr= {"D:\\Demo2.csv","D:\\Demo1.csv"};
		List<Pair<Text, Text>> output = null;
		List<Pair<Text, Text>> output2 = null;
		//Pair<Text, Text> p;
		StringBuffer contents = new StringBuffer();
		for (int i=0; i<bothstr.length;i++)
		{
			File csvFile  = new File(bothstr[i].toString().trim());
		    
		    BufferedReader reader = null;

		    reader = new BufferedReader(new FileReader(csvFile));
		    String text = null;

		    // repeat until all lines is read
		    while ((text = reader.readLine()) != null) {
		      contents.append(text).append(System.getProperty("line.separator"));
		    }
		    reader.close();
		}   
		mapReduceDriver1.addInput(new Pair<Object, Text>("1", new Text(contents.toString())));
		    
		    output = mapReduceDriver1.run();
			
			for (Pair<Text, Text>  p: output) {
				System.out.println(p.getFirst() + " - " + p.getSecond());
						
				
				mapReduceDriver2.addInput(new Pair<Text, Text>(p.getFirst(), p.getSecond()));
			}	
			
			System.out.println("================================");
			output2= mapReduceDriver2.run();
			for (Pair<Text, Text> p2 : output2) {
				System.out.println(p2.getFirst() + " - " + p2.getSecond());
		}
			
			//System.out.println("output"+output.size());
			}
	
}
