package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PMVal implements org.apache.hadoop.io.Writable{
	private float value;
	private int count;

	public void setValue(float value){
		this.value = value;
	}

	public float getValue(){
		return value;
	}

	public void setCount(int count){
		this.count = count;
	}

	public int getCount(){
		return count;
	}

	public String toString()
	{
		float avg = value/count;
		return "" + avg;
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		value = in.readFloat();
		count = in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException{
		out.writeFloat(value);
		out.writeInt(count);
	}
}
