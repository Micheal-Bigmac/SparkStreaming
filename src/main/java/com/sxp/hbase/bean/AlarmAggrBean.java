/**  
 * @Title AlarmAggr.java
 * @Package com.hsae.task.bolt
 * @Description TODO(用一句话描述该文件做什么)
 * @author 韩欣宇
 * @date: 2016年2月24日 上午11:07:33
 * @company 上海势航网络科技有限公司
 * @version V1.0  
 */
package com.sxp.hbase.bean;

import com.google.protobuf.InvalidProtocolBufferException;
import com.sxp.task.protobuf.generated.AlarmAggrInfo2.AlarmAggr;
import com.sxp.task.protobuf.generated.AlarmAggrInfo2.AlarmAggr.Builder;

/**
 * @ClassName AlarmAggrBean
 * @Description 报警聚合bean
 * @author 韩欣宇
 * @company 上海势航网络科技有限公司
 * @date 2016年2月24日 上午11:07:33
 */
public class AlarmAggrBean {
	private long id;
	private long alarmTypeId;
	private long vehicleId;
	private String plateCode;
	private long startTime;
	private long endTime;
	private int duration;
	private int count;
	private long startLocationId;
	private long endLocationId;
	private String remark;
	private float avgSpeed;
	private float avgDSpeed;
	private String enterpriseCode;

	public AlarmAggrBean(long vehicleId, long alarmTypeId) {
		this.vehicleId = vehicleId;
		this.alarmTypeId = alarmTypeId;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getAlarmTypeId() {
		return alarmTypeId;
	}

	public void setAlarmTypeId(long alarmTypeId) {
		this.alarmTypeId = alarmTypeId;
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

	public String getEnterpriseCode() {
		return enterpriseCode;
	}

	public void setEnterpriseCode(String enterpriseCode) {
		this.enterpriseCode = enterpriseCode;
	}

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
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

	/**
	 * @Title equals
	 * @Description
	 * @param obj
	 * @return
	 * @see Object#equals(Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof AlarmAggrBean) {
			AlarmAggrBean a = (AlarmAggrBean) obj;
			return this.getVehicleId() == a.getVehicleId() && this.getAlarmTypeId() == a.getAlarmTypeId();
		}
		return false;
	}

	/**
	 * @Title toByteArray
	 * @Description protobuf
	 * @return byte[]
	 * @author 韩欣宇
	 * @date 2016年2月24日 下午5:20:57
	 */
	public byte[] toByteArray() {
		Builder a = AlarmAggr.newBuilder();
		a.setId(this.getId());
		a.setAlarmTypeId(this.getAlarmTypeId());
		a.setVehicleId(this.getVehicleId());
		a.setStartTime(this.getStartTime());
		a.setEndTime(this.getEndTime());
		a.setDuration(this.getDuration());
		a.setCount(this.getCount());
		a.setStartLocationId(this.getStartLocationId());
		a.setEndLocationId(this.getEndLocationId());
		a.setAvgSpeed(this.getAvgSpeed());
		a.setAvgDSpeed(this.getAvgDSpeed());
		return a.build().toByteArray();
	}

	/**
	 * @Title parseFrom
	 * @Description parseFrom
	 * @param bytes
	 * @return AlarmAggrBean
	 * @throws InvalidProtocolBufferException
	 * @author 韩欣宇
	 * @date 2016年2月24日 下午5:24:05
	 */
	public AlarmAggrBean parseFrom(byte[] bytes) throws InvalidProtocolBufferException {
		AlarmAggr pb = AlarmAggr.parseFrom(bytes);
		AlarmAggrBean a = new AlarmAggrBean(pb.getVehicleId(), pb.getAlarmTypeId());
		a.setId(pb.getId());
		a.setStartTime(pb.getStartTime());
		a.setEndTime(pb.getEndTime());
		a.setDuration(pb.getDuration());
		a.setCount(pb.getCount());
		a.setStartLocationId(pb.getStartLocationId());
		a.setEndLocationId(pb.getEndLocationId());
		a.setAvgSpeed(pb.getAvgSpeed());
		a.setAvgDSpeed(pb.getAvgDSpeed());
		return a;
	}
}
