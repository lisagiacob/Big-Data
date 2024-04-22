package it.polito.bigdata.hadoop; 
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StatisticsWritable implements org.apache.hadoop.io.Writable {
	/* Private variables */
	private float sum = 0;
	private int count = 0;

	public float getSum() {
		return sum;
	}
	public void setSum(float sum) {
		this.sum = sum;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}

	@Override /* Serialize the fields of this object to out */ 
	public void write(DataOutput out) throws IOException {
		out.writeFloat(sum);
		out.writeInt(count); 
	}
	
	@Override /* Deserialize the fields of this object from in */
	public void readFields(DataInput in) throws IOException {
		sum=in.readFloat();
		count=in.readInt(); 
	}

	/* Specify how to convert the contents of the instances of this class to a String
	* Useful to specify how to store/write the content of this class * in a textual file */
	public String toString() {
		double avg = sum / count;
		String formattedString = new String(""+ avg);
		return formattedString; 
	}
}