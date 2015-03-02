package dna2;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class DNA2Mapper extends Mapper<Object, Text, Text, Text >
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
			String retval=revandmin((split[i+1]).toString().trim());
			key1.set(retval); 
			context.write(key1,value1);
			i++;
            
		}
	}

	private String revandmin(String dnastr) {
		
		String[] bothstr= {dnastr,new StringBuffer(dnastr).reverse().toString()};
    	int[] array = new int[2];
    	
    	      for (int k=0;k<bothstr.length;k++)
    	      {
    	      String newtext = bothstr[k].toString();
    	      int val=0;
    	      int j=newtext.length();
    	     
    			for(int i=0 ; i < newtext.length(); i++)
    			  {
    				  char ch = newtext.charAt(i);
    				  int ascii = (int) ((int) ch * Math.pow(10, j)) ;
    				  val+=ascii;
    				  j-=1;
    			  }
    			
    			array[k]=val;
    	      }
    	      
    	      String small=null;
    	      
    	      if(Math.min(array[0],array[1])==array[0])
    	          small=bothstr[0];
    	      else
    	    	  small= bothstr[1];
    	      
		return small;
		
		
	}
    
}
