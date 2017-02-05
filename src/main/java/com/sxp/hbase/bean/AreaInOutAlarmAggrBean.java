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
import com.sxp.task.protobuf.generated.AlarmAggrInfo2.AreaInout;
import com.sxp.task.protobuf.generated.AlarmAggrInfo2.AreaInout.Builder;

/**
 * @ClassName AreaInOutAlarmAggrBean
 * @Description 进出区域报警聚合
 * @author 韩欣宇
 * @company 上海势航网络科技有限公司
 * @date 2016年2月24日 上午11:07:33
 */
public class AreaInOutAlarmAggrBean extends AlarmAggrBean {

	private int appendId;
	/**
	 * 区域Id
	 */
	private int areaId; 
	/**
	 * 位置类型：比如，'矩形'还是'圆形'
	 */
	private int locationType;
	/**
	 * 方向，进还是出
	 */
	private boolean direction;

	/**
	 * @param vehicleId
	 * @param alarmTypeId
	 */
	public AreaInOutAlarmAggrBean(long vehicleId, long alarmTypeId) {
		super(vehicleId, alarmTypeId);
	}

	public int getAppendId() {
		return appendId;
	}

	public void setAppendId(int appendId) {
		this.appendId = appendId;
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

	public boolean isDirection() {
		return direction;
	}

	public void setDirection(boolean direction) {
		this.direction = direction;
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
		if (obj instanceof AreaInOutAlarmAggrBean) {
			AreaInOutAlarmAggrBean a = (AreaInOutAlarmAggrBean) obj;
			return this.getVehicleId() == a.getVehicleId() && this.getAlarmTypeId() == a.getAlarmTypeId() && this.getAreaId() == a.getAreaId();
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
		Builder a = AreaInout.newBuilder();
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
		a.setAreaId(this.getAreaId());
		a.setLocationType(this.getLocationType());
		a.setDirection(this.isDirection());
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
	public AreaInOutAlarmAggrBean parseFrom(byte[] bytes) throws InvalidProtocolBufferException {
		AreaInout pb = AreaInout.parseFrom(bytes);
		AreaInOutAlarmAggrBean a = new AreaInOutAlarmAggrBean(pb.getVehicleId(), pb.getAlarmTypeId());
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
		a.setAreaId(pb.getAreaId());
		a.setLocationType(pb.getLocationType());
		a.setDirection(pb.getDirection());
		return a;
	}
}
