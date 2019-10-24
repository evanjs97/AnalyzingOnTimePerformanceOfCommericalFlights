package hadoop.util;

public class DoubleAveragePair {
	private int count;
	private double average;

	public int getCount() {
		return count;
	}

	public double getRunningSum() {
		return average;
	}

	public double getAverage() {
		if(count == 0) return 0;
		return average / (double) count;
	}

	public DoubleAveragePair(double average, int count) {
		this.average = average;
		this.count = count;
	}

	public void addToCount(int add) { count += add; }

	public void addToAverage(double add) { average += add; }
}
