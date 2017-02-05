package com.sxp.task.bolt.push;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Vehicle implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1640495036119951711L;
	
	private String id; 
	private String plateCode;	//车牌号
	private Map<String,User> userMap = new HashMap();
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getPlateCode() {
		return plateCode;
	}
	public void setPlateCode(String plateCode) {
		this.plateCode = plateCode;
	}
	public Map<String, User> getUserMap() {
		return userMap;
	}
	public void setUserMap(Map<String, User> userMap) {
		this.userMap = userMap;
	}
	
	
	

}
