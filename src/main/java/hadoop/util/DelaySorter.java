package hadoop.util;

public class DelaySorter implements Comparable<DelaySorter>{

	int year;
	int month;
	int dayOfMonth;
	String timeOfDay;
	int lateDelay;
	int secDelay;
	int nasDelay;
	int weatherDelay;
	int carrierDelay;
	String carrierCode;
	String airportCode;


	public DelaySorter(LateAircraftWritable law) {
		year = law.getYear().get();
		month = law.getMonth().get();
		dayOfMonth = law.getDayOfMonth().get();
		timeOfDay = law.getTimeOfDay().toString();
		lateDelay = law.getLateAircraftDelay().get();
		secDelay = law.getLateAircraftDelay().get();
		nasDelay = law.getNASDelay().get();
		weatherDelay = law.getWeatherDelay().get();
		carrierDelay = law.getCarrierDelay().get();
		carrierCode = law.getCarrierCode().toString();
		airportCode = law.getAirportCode().toString();
	}

	public DelaySorter(int year, int month, int dayOfMonth, String timeOfDay, int lateDelay, int secDelay, int nasDelay, int weatherDelay, int carrierDelay, String carrierCode, String airportCode) {
		this.year = year;
		this.month = month;
		this.dayOfMonth = dayOfMonth;
		this.timeOfDay = timeOfDay;
		this.lateDelay = lateDelay;
		this.secDelay = secDelay;
		this.nasDelay = nasDelay;
		this.weatherDelay = weatherDelay;
		this.carrierDelay = carrierDelay;
		this.carrierCode = carrierCode;
		this.airportCode = airportCode;
	}

	@Override
	public int compareTo(DelaySorter o) {
		int yearC = Integer.compare(year, o.year);
		if(yearC != 0) return yearC;
		int monthC = Integer.compare(month, o.month);
		if(monthC != 0) return monthC;
		int dayC = Integer.compare(dayOfMonth, o.dayOfMonth);
		if(dayC != 0) return dayC;
		return timeOfDay.compareTo(o.timeOfDay);
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(timeOfDay);
		builder.append(',');
		builder.append(dayOfMonth);
		builder.append(',');
		builder.append(month-1);
		builder.append(',');
		builder.append(year);
		builder.append(',');
		builder.append(lateDelay);
		builder.append(',');
		builder.append(secDelay);
		builder.append(',');
		builder.append(nasDelay);
		builder.append(',');
		builder.append(weatherDelay);
		builder.append(',');
		builder.append(carrierDelay);
		builder.append(',');
		builder.append(carrierCode);
		builder.append(',');
		builder.append(airportCode);
		return builder.toString();
	}
}
