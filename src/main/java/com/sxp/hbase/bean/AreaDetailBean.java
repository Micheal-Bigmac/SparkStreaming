package com.sxp.hbase.bean;

/**
 * @ClassName AreaDetailBean
 * @Description 位置属性
 * @author 熊尧
 * @company 上海势航网络科技有限公司
 * @date 2016年7月1日
 */
public class AreaDetailBean {
	
	private long areaId ;    //区域ID
	private int locationType;//位置类型：比如，'矩形',方型，圆形，多边形。
	private long lat;        //纬度
	private long lng;        //经度
	private int radius;     //区域半径
	public long getAreaId() {
		return areaId;
	}
	public void setAreaId(long areaId) {
		this.areaId = areaId;
	}
	public int getLocationType() {
		return locationType;
	}
	public void setLocationType(int locationType) {
		this.locationType = locationType;
	}
	public long getLat() {
		return lat;
	}
	public void setLat(long lat) {
		this.lat = lat;
	}
	public long getLng() {
		return lng;
	}
	public void setLng(long lng) {
		this.lng = lng;
	}
	public int getRadius() {
		return radius;
	}
	public void setRadius(int radius) {
		this.radius = radius;
	}
	
	
	

}

