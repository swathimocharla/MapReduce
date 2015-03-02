package dna2;


public class RevandMin {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String string="abcdef";
	    	      
	    	String[] bothstr= {string,new StringBuffer(string).reverse().toString()};
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
	    	      int minval= Math.min(array[0],array[1]);
	    	      String small=null;
	    	      
	    	      if(Math.min(array[0],array[1])==array[0])
	    	          small=bothstr[0];
	    	      else
	    	    	  small= bothstr[1];
	    	      
	    	      System.out.println(small);
	    	      
	}
}
