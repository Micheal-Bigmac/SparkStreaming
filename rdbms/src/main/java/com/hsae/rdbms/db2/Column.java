package com.hsae.rdbms.db2;

import java.io.Serializable;

public class Column implements Serializable{
	private String name;
	private int type;
	private int scaleOrLength;

	public Column(String name, int type) {
		super();
		this.name = name;
		this.type = type;
	}

	public Column(String name, int type, int scaleOrLength) {
		super();
		this.name = name;
		this.type = type;
		this.scaleOrLength = scaleOrLength;
	}

	public String getName() {
		return name;
	}

	public int getType() {
		return type;
	}

	public int getScaleOrLength() {
		return scaleOrLength;
	}

}
