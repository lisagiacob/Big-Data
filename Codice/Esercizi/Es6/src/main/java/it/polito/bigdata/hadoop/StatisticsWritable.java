package it.polito.bigdata.hadoop; 
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StatisticsWritable implements org.apache.hadoop.io.Writable {
	/* Private variables */
	private float min = 0;
	private float max = 0;

	public float getMin() {
		return min;
	}
	public void setMin(float min) {
		this.min = min;
	}
	public float getMax() {
		return max;
	}
	public void setMax(float max) {
		this.max = max;
	}

	@Override /* Serialize the fields of this object to out */ 
	public void write(DataOutput out) throws IOException {
		out.writeFloat(min);
		out.writeFloat(max); 
	}
	
	@Override /* Deserialize the fields of this object from in */
	public void readFields(DataInput in) throws IOException {
		min=in.readFloat();
		max=in.readFloat(); 
	}

	/* Specify how to convert the contents of the instances of this class to a String
	* Useful to specify how to store/write the content of this class * in a textual file */
	public String toString() {
		String formattedString = new String(min + "_"+ max);
		return formattedString; 
	}
}