package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class cityCountry implements org.apache.hadoop.io.Writable{
	private int countCity, countCountry;
	private String nation;

	public void setCountCity(int countCity){
		this.countCity = countCity;
	}

	public void setCountNat(int countCity){
		this.countCountry = countCity;
	}

	public void setNation(String nation){
		this.nation = nation;
	}

	public String getNation(){
		return nation;
	}

	public int getCountCity(){
		return countCity;
	}

	public int getCountNat(){
		return countCountry;
	}

	public String toString()
	{
		return nation + "\t" + countCity + "\t" + countCountry;
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		countCity = in.readInt();
		countCountry = in.readInt();
		nation = in.readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException{
		out.writeInt(countCity);
		out.writeInt(countCountry);
		out.writeUTF(nation);
	}
}
