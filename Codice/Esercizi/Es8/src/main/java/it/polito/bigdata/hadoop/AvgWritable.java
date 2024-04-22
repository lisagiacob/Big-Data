package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgWritable implements org.apache.hadoop.io.Writable
{
	private float sum;
	private int count, month;
	private boolean year = false;

	public int getMonth()
	{
		return month;
	}

	public void setMonth(int month)
	{
		this.month = month;
	}
	
	public float getSum()
	{
		return sum;
	}

	public void setSum(float sum)
	{
		this.sum = sum;
	}

	public int getCount()
	{
		return count;
	}

	public void setCount(int count)
	{
		this.count = count;
	}

	public void setYear(){
		this.year = true;
	}
	

	@Override
	public void readFields(DataInput in) throws IOException {
		sum = in.readFloat();
		count = in.readInt();
		month = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(sum);
		out.writeInt(count);
		out.writeInt(month);
	}
	
	public String toString()
	{
		String formattedString;
		if(year){
			formattedString=new String("Monthly avg: " + sum/count);
		}
		else{ 
			formattedString=new String("Income of the month: " + sum);
		}

		return formattedString;
	}
	
}
