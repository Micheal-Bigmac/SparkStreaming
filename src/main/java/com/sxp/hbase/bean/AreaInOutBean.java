package com.sxp.hbase.bean;

import com.google.protobuf.InvalidProtocolBufferException;
import com.sxp.task.protobuf.generated.AreaInoutBeanInfo.AreaInoutBean;

/**
 * @ClassName  AreaInOutBean
 * @Description 车辆进入特定区域，计算一段时间内在该区域的最大速度，最小速度，平均速度，持续时间
 * @author 熊尧
 * @company 上海势航网络科技有限公司
 * @date 2016年6月29日
 */
public class AreaInOutBean {
	
	private long id;
	private int areaId; 
	private int locationType;//位置类型：比如，'矩形'还是'圆形'
	private long vehicleId;
	private String plateCode;
	private long startTime;
	private long endTime;
	private long startLocationId;
	private long endLocationId;
	private int duration;
	private int count;
	private float avgSpeed;
	private float avgDSpeed;
	private float maxSpeed;
	private float maxDSpeed;
	private float minSpeed;
	private float minDSpeed;
	
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	
	public int getAreaId() {
		return areaId;
	}
	public void setAreaId(int areaId) {
		this.areaId = areaId;
	}
	public int getLocationType() {
		return locationType;
	}
	public void setLocationType(int locationType) {
		this.locationType = locationType;
	}
	public long getVehicleId() {
		return vehicleId;
	}
	public void setVehicleId(long vehicleId) {
		this.vehicleId = vehicleId;
	}
	public String getPlateCode() {
		return plateCode;
	}
	public void setPlateCode(String plateCode) {
		this.plateCode = plateCode;
	}
	public long getStartTime() {
		return startTime;
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public long getEndTime() {
		return endTime;
	}
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	public int getDuration() {
		return duration;
	}
	public void setDuration(int duration) {
		this.duration = duration;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public float getAvgSpeed() {
		return avgSpeed;
	}
	public void setAvgSpeed(float avgSpeed) {
		this.avgSpeed = avgSpeed;
	}
	public float getAvgDSpeed() {
		return avgDSpeed;
	}
	public void setAvgDSpeed(float avgDSpeed) {
		this.avgDSpeed = avgDSpeed;
	}
	public float getMaxSpeed() {
		return maxSpeed;
	}
	public void setMaxSpeed(float maxSpeed) {
		this.maxSpeed = maxSpeed;
	}
	public float getMaxDSpeed() {
		return maxDSpeed;
	}
	public void setMaxDSpeed(float maxDSpeed) {
		this.maxDSpeed = maxDSpeed;
	}
	public float getMinSpeed() {
		return minSpeed;
	}
	public void setMinSpeed(float minSpeed) {
		this.minSpeed = minSpeed;
	}
	public float getMinDSpeed() {
		return minDSpeed;
	}
	public void setMinDSpeed(float minDSpeed) {
		this.minDSpeed = minDSpeed;
	}
	public long getStartLocationId() {
		return startLocationId;
	}
	public void setStartLocationId(long startLocationId) {
		this.startLocationId = startLocationId;
	}
	public long getEndLocationId() {
		return endLocationId;
	}
	public void setEndLocationId(long endLocationId) {
		this.endLocationId = endLocationId;
	}
	/**
	 * @description 将对象序列化为probutfer二进制文件
	 * @return
	 */
	public byte[] toByteArray(){
		AreaInoutBean.Builder a  = AreaInoutBean.newBuilder();
		a.setAreaId(this.getAreaId());
		a.setAvgDSpeed(this.getAvgDSpeed());
		a.setAvgSpeed(this.getAvgSpeed());
		a.setCount(this.getCount());
		a.setDuration(this.getDuration());
		a.setEndLocationId(this.getEndLocationId());
		a.setEndTime(this.getEndTime());
		a.setVehicleId(this.getVehicleId());
		a.setID(this.getId());
		a.setLocationType(this.getLocationType());
		a.setMaxDSpeed(this.getMaxDSpeed());
		a.setMaxSpeed(this.getMaxSpeed());
		a.setMinDSpeed(this.getMinDSpeed());
		a.setMinSpeed(this.getMinSpeed());
		a.setPlateCode(this.plateCode);
		a.setStartLocationId(this.getStartLocationId());
		a.setStartTime(this.getStartTime());
		return a.build().toByteArray();
	}
	
	public  AreaInOutBean parseFrom(byte[] bytes) throws InvalidProtocolBufferException{
		AreaInoutBean b = AreaInoutBean.parseFrom(bytes);
		AreaInOutBean a = new AreaInOutBean();
		a.setAreaId(b.getAreaId());
		a.setAvgDSpeed(b.getAvgDSpeed());
		a.setAvgSpeed(b.getAvgSpeed());
		a.setCount(b.getCount());
		a.setDuration(b.getDuration());
		a.setEndLocationId(b.getEndLocationId());
		a.setEndTime(b.getEndTime());
		a.setVehicleId(b.getVehicleId());
		a.setId(b.getID());
		a.setLocationType(b.getLocationType());
		a.setMaxDSpeed(b.getMaxDSpeed());
		a.setMaxSpeed(b.getMaxSpeed());
		a.setMinDSpeed(b.getMinDSpeed());
		a.setMinSpeed(b.getMinSpeed());
		a.setPlateCode(b.getPlateCode());
		a.setStartLocationId(b.getStartLocationId());
		a.setStartTime(b.getStartTime());
		return a;
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
}
