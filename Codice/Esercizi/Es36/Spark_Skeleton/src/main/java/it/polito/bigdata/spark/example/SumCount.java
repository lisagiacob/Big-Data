package it.polito.bigdata.spark.example;

import java.io.Serializable;

class SumCount implements Serializable {
	public double sum;
	public int numElements;
	public SumCount(int sum, int numElements) {
		this.sum = sum;
		this.numElements = numElements;
	}
	public double avg() {
		return sum/ (double) numElements;
	}
}