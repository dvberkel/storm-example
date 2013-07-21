package org.effrafax.storm;

public enum Measure {
	SECONDS(1000L), MILLISECONDS(1);
	
	private final long milliSeconds;

	Measure(long milliSeconds) {
		this.milliSeconds = milliSeconds;
	}

	public long times(int amount) {
		return milliSeconds * amount;
	}

}
