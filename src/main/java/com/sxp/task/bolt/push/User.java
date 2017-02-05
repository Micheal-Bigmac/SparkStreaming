package com.sxp.task.bolt.push;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class User implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5091289621898086715L;

	private String id;	//userId
	private String fullName;	//用户姓名
	private String mobilePhone;	//手机号码
	private String alarmList;	//报警提醒列表
	private String createDate;	//创建日期
	
	private Map deviceMap = new HashMap();
	
	
	User(){
		Date date = new Date();
		this.createDate = date.toString();
	}
		
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getFullName() {
		return fullName;
	}
	public void setFullName(String fullName) {
		this.fullName = fullName;
	}
	public String getMobilePhone() {
		return mobilePhone;
	}
	public void setMobilePhone(String mobilePhone) {
		this.mobilePhone = mobilePhone;
	}
	public String getAlarmList() {
		return alarmList;
	}
	public void setAlarmList(String alarmList) {
		this.alarmList = alarmList;
	}
	public String getCreateDate() {
		return createDate;
	}
	public void setCreateDate(String createDate) {
		this.createDate = createDate;
	}
	public Map getDeviceMap() {
		return deviceMap;
	}

	public void setDeviceMap(Map deviceMap) {
		this.deviceMap = deviceMap;
	}
}
