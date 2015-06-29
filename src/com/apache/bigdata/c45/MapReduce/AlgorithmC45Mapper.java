package com.apache.bigdata.c45.MapReduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.apache.bigdata.c45.AlgorithmC45;
import com.apache.bigdata.c45.datamodel.Data;

public class AlgorithmC45Mapper extends Mapper<Object, Text, Text, IntWritable> {

private final static IntWritable one = new IntWritable(1);
private Text attValue = new Text();
private int i;
private String token;
public static int no_Attr;


public void map(Object key, Text value, Context context) throws InterruptedException,IOException {
	
	 
	  Data data=null;
	  int size_split=0;
	  data=AlgorithmC45.currentsplit;
	  String line = value.toString();      //changing input instance value to string
	  StringTokenizer itr = new StringTokenizer(line,",");
	  int index=0;
	  String attr_value=null;
	  no_Attr=itr.countTokens()-1;
	  String attr[]=new String[no_Attr];
	  boolean match=true;
	  for(i =0;i<no_Attr;i++)
		  {
			  attr[i]=itr.nextToken();		//Finding the values of different attributes
		  }
	  String classLabel=itr.nextToken();
	  size_split=data.att_index.size();
	  
	  for(int count=0;count<size_split;count++)
	  	 {
		  index=(Integer) data.att_index.get(count);
		  attr_value=(String)data.att_value.get(count);
		  
			  if(attr[index].equals(attr_value))   //may also use attr[index][z][1].contentEquals(attr_value)
			  {
				  		
			  }
			  else
			  {
			
				  	match=false;
				  	break;
			  }
	  	  }
	  if(match)
	  {
		  for(int l=0;l<no_Attr;l++)
		  {  
			  if(data.att_index.contains(l))
			  {
			  }
			  else
			  {
			  token=l+" "+attr[l]+" "+classLabel;
			  attValue.set(token);
			  context.write(attValue, one);
			  }
		  }
		  if(size_split==no_Attr)
		  {
			  token=no_Attr+" "+"null"+" "+classLabel;
			  attValue.set(token);
			  context.write(attValue, one);
	  	  }
	  }
 	}
}

  
