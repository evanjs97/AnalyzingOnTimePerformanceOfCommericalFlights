package hadoop.util;

public class DelaySummarizeSorter {
	private int lateDelay;
	private int numLateDelay;
	private int secDelay;
	private int numSecDelays;
	private int nasDelay;
	private int numNASDelays;
	private int weatherDelay;
	private int numWeatherDelays;
	private int carrierDelay;
	private int numCarrierDelays;
	private int numFlights;
	private String name;

	public int getNumLateDelay() {
		return numLateDelay;
	}

	public double getLateDelayPercent() {
		return ((double)numLateDelay / numFlights) * 100;
	}

	public double getCarrierDelayPercent() {
		return ((double)numCarrierDelays / numFlights) * 100;
	}

	public double getNASDelayPercent() {
		return ((double)numNASDelays / numFlights * 100);
	}

	public double getSecDelayPercent() {
		return ((double)numSecDelays / numFlights * 100);
	}

	public double getWeatherDelayPercent() {
		return ((double)numWeatherDelays / numFlights * 100);
	}

	public int getNumSecDelays() {
		return numSecDelays;
	}

	public int getNumNASDelays() {
		return numNASDelays;
	}

	public int getNumWeatherDelays() {
		return numWeatherDelays;
	}

	public int getNumCarrierDelays() {
		return numCarrierDelays;
	}

	public String getName() {
		return name;
	}

	public int getLateDelay() {
		return lateDelay;
	}

	public int getSecDelay() {
		return secDelay;
	}

	public int getNasDelay() {
		return nasDelay;
	}

	public int getWeatherDelay() {
		return weatherDelay;
	}

	public int getCarrierDelay() {
		return carrierDelay;
	}

	public int getNumFlights() {
		return numFlights;
	}

	public DelaySummarizeSorter(int lateDelay, int numLateDelays, int secDelay, int numSecDelays, int nasDelay,
								int numNASDelays, int weatherDelay, int numWeatherDelays, int carrierDelay,
								int numCarrierDelays, String name, int numFlights) {
		this.lateDelay = lateDelay;
		this.numLateDelay = numLateDelays;
		this.secDelay = secDelay;
		this.numSecDelays = numSecDelays;
		this.nasDelay = nasDelay;
		this.numNASDelays = numNASDelays;
		this.weatherDelay = weatherDelay;
		this.numWeatherDelays = numWeatherDelays;
		this.carrierDelay = carrierDelay;
		this.numCarrierDelays = numCarrierDelays;
		this.name = name;
		this.numFlights = numFlights;
	}
}
