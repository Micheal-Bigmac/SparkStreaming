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
import com.sxp.task.protobuf.generated.AlarmAggrInfo2.OverTime;
import com.sxp.task.protobuf.generated.AlarmAggrInfo2.OverTime.Builder;

/**
 * @ClassName OverTimeAlarmAggrBean
 * @Description 超时/不足报警聚合
 * @author 韩欣宇
 * @company 上海势航网络科技有限公司
 * @date 2016年2月24日 上午11:07:33
 */
public class OverTimeAlarmAggrBean extends AlarmAggrBean {
	/**
	 * @param vehicleId
	 * @param alarmTypeId
	 */
	public OverTimeAlarmAggrBean(long vehicleId, long alarmTypeId) {
		super(vehicleId, alarmTypeId);
	}

	private int appendId;
	private int roadId;
	private int driveTime;
	private boolean result;

	public int getAppendId() {
		return appendId;
	}

	public void setAppendId(int appendId) {
		this.appendId = appendId;
	}

	public int getRoadId() {
		return roadId;
	}

	public void setRoadId(int roadId) {
		this.roadId = roadId;
	}

	public int getDriveTime() {
		return driveTime;
	}

	public void setDriveTime(int driveTime) {
		this.driveTime = driveTime;
	}

	public boolean isResult() {
		return result;
	}

	public void setResult(boolean result) {
		this.result = result;
	}

	/**
	 * @Title toByteArray
	 * @Description protobuf
	 * @return byte[]
	 * @author 韩欣宇
	 * @date 2016年2月24日 下午5:20:57
	 */
	public byte[] toByteArray() {
		Builder a = OverTime.newBuilder();
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
		a.setAppendId(this.getAppendId());
		a.setRoadId(this.getRoadId());
		a.setRoadId(this.getRoadId());
		a.setDriveTime(this.getDriveTime());
		a.setResult(this.isResult());
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
	public OverTimeAlarmAggrBean parseFrom(byte[] bytes) throws InvalidProtocolBufferException {
		OverTime pb = OverTime.parseFrom(bytes);
		OverTimeAlarmAggrBean a = new OverTimeAlarmAggrBean(pb.getVehicleId(), pb.getAlarmTypeId());
		a.setId(pb.getId());
		a.setStartTime(pb.getStartTime());
		a.setEndTime(pb.getEndTime());
		a.setDuration(pb.getDuration());
		a.setCount(pb.getCount());
		a.setStartLocationId(pb.getStartLocationId());
		a.setEndLocationId(pb.getEndLocationId());
		a.setAvgSpeed(pb.getAvgSpeed());
		a.setAvgDSpeed(pb.getAvgDSpeed());
		a.setAppendId(pb.getAppendId());
		a.setRoadId(pb.getRoadId());
		a.setDriveTime(pb.getDriveTime());
		a.setResult(pb.getResult());
		return a;
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
		if (obj instanceof OverTimeAlarmAggrBean) {
			OverTimeAlarmAggrBean a = (OverTimeAlarmAggrBean) obj;
			return this.getVehicleId() == a.getVehicleId() && this.getAlarmTypeId() == a.getAlarmTypeId() && this.getRoadId() == a.getRoadId();
		}
		return false;
	}
}
