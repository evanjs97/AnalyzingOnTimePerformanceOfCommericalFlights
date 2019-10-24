package hadoop.util;

public class StringValue {
	private String key;
	private double value;

	public String getKey() {
		return key;
	}

	public double getValue() {
		return value;
	}

	public StringValue(String key, double value) {
		this.key = key;
		this.value = value;
	}

	public void addToValue(double add) { value += add; }
}
