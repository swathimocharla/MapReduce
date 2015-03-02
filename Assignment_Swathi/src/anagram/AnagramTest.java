package anagram;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;


import org.apache.hadoop.io.ArrayWritable;
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

public class AnagramTest {

	
	MapDriver<Object, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;
	
	
	
	@Before
	public void setUp() 
	{

		/*
		 * Set up the mapper test harness.
		 */
		AnagramMapper mapper = new AnagramMapper();
		mapDriver = new MapDriver<Object, Text , Text,  Text>();
		mapDriver.setMapper(mapper);
		
		
		AnagramReducer reducer = new AnagramReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);	
		
	}
	
	
	//@Test
	public void testMap() throws IOException {

		mapDriver.setInput(new Pair<Object, Text>("1", new Text(
				"the tic tac toe act het ict")));
		/*mapDriver.setInput(new Pair<Object, Text>("2", new Text(
				"is cat act toe")));*/
		List<Pair<Text, Text>> output = mapDriver.run();
		
		for (Pair<Text, Text> p : output) {
			System.out.println("===================");
			//System.out.println(p.getFirst());
			System.out.println(p.getFirst() + " - " + p.getSecond());
		}
	}
	
	@Test
	public void testMapReduce() throws IOException {

		mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text(
				"the tic tac toe act het cit")));
		mapReduceDriver.addInput(new Pair<Object, Text>("2", new Text(
				"is cat act toe")));
		List<Pair<Text, Text>> output = mapReduceDriver.run();
		
		for (Pair<Text, Text> p : output) {
			System.out.println(p.getFirst() + " - " + p.getSecond());
		}
	}
}
