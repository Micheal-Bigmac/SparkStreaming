package com.sxp.task.geodesc;

import java.util.List;

public class ResponseList {
	private int status;
	private List<Response> geocoderResponses;

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public List<Response> getGeocoderResponses() {
		return geocoderResponses;
	}

	public void setGeocoderResponses(List<Response> geocoderResponses) {
		this.geocoderResponses = geocoderResponses;
	}

}
