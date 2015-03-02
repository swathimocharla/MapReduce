package dna2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class DNA2Test {




	MapDriver<Object, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;

	
	@Before
	public void setUp() 
	{

		/*
		 * Set up the mapper test harness.
		 */
		DNA2Mapper mapper = new DNA2Mapper();
		mapDriver = new MapDriver<Object, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		
		Reducer<Text, Text, Text, Text> reducer = new DNA2Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);	
		
	}
	
	
	//@Test
	public void testMap() throws IOException {

		
		File csvFile  = new File("D:\\new.csv");
	    StringBuffer contents = new StringBuffer();
	    BufferedReader reader = null;

	    reader = new BufferedReader(new FileReader(csvFile));
	    String text = null;

	    // repeat until all lines is read
	    while ((text = reader.readLine()) != null) {
	      contents.append(text).append(System.getProperty("line.separator"));
	    }
	    reader.close();
	    //System.out.println(contents.toString());
	    
	   // System.out.println(contents.toString());
	    
	    mapDriver.setInput(new Pair<Object, Text>("1", new Text(contents.toString())));
	    /*System.out.println(mapDriver.getInputValue());
	    System.out.println(mapDriver.getInputKey());*/
	    
	    System.out.println(contents.toString());
	    List<Pair<Text, Text>> output = mapDriver.run();
		
		for (Pair<Text, Text> p : output) {
			System.out.println(p.getFirst() + " - " + p.getSecond());
			
			
		}
	}
	
	@Test
	public void testMapReduce() throws IOException {

		
		File csvFile  = new File("D:\\new.csv");
	    StringBuffer contents = new StringBuffer();
	    BufferedReader reader = null;

	    reader = new BufferedReader(new FileReader(csvFile));
	    String text = null;

	    // repeat until all lines is read
	    while ((text = reader.readLine()) != null) {
	      contents.append(text).append(System.getProperty("line.separator"));
	    }
	    reader.close();
	    //System.out.println(contents.toString());
	    
	    mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text(contents.toString())));
	    List<Pair<Text, Text>> output = mapReduceDriver.run();
		for (Pair<Text, Text> p : output) {
			System.out.println(p.getFirst() + " - " + p.getSecond());
			
			
		}
	}

}
