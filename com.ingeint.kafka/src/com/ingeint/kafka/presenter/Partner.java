package com.ingeint.kafka.presenter;

public class Partner {
	String uu;
	String name;

	public String getUu() {
		return uu;
	}

	public void setUu(String uu) {
		this.uu = uu;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return String.format("Partner [uu=%s, name=%s]", uu, name);
	}

}
