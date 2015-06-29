package com.apache.bigdata.c45.datamodel;

import java.util.ArrayList;
import java.util.List;


public class Data implements Cloneable{
	public List<Integer> att_index;
	public List<String> att_value;
	double entophy;
	private String classLabel;
	public Data()
	{
		 this.att_index= new ArrayList<Integer>();
		 this.att_value = new ArrayList<String>();
	}
	
	
	Data(List<Integer> attr_index,List<String> attr_value)
	{
		this.att_index=attr_index;
		this.att_value=attr_value;
	}
	
	
	void add(Data obj)
	{
		this.add(obj);
	}
	
	
	public String getClassLabel() {
		return classLabel;
	}
	
	
	public void setClassLabel(String classLabel) {
		this.classLabel = classLabel;
	}
}