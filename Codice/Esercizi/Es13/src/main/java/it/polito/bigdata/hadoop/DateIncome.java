package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateIncome implements org.apache.hadoop.io.Writable{
	private float income;
	private String date;

	public void setIncome(float income){
		this.income = income;
	}

	public float getIncome(){
		return income;
	}

	public void setDate(String date){
		this.date = date;
	}

	public String getDate(){
		return date;
	}

	public String toString()
	{
		return date + "\t" + income;
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		income = in.readFloat();
		date = in.readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException{
		out.writeFloat(income);
		out.writeUTF(date);
	}
}
