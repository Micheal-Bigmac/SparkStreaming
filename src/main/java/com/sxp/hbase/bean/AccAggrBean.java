/**
 * @ClassName 
 * @Description 
 * @author 李小辉
 * @company 上海势航网络科技有限公司
 * @date 2016年9月8日
 */
package com.sxp.hbase.bean;

import com.google.protobuf.InvalidProtocolBufferException;
import com.sxp.task.protobuf.generated.AccAggrInfo;

public class AccAggrBean {
	private long id;
	private long accTypeId;
	private long vehicleId;
	private long startTime;
	private long endTime;
	private int count;
	
	public AccAggrBean(long vehicleId, long accTypeId){
		this.vehicleId = vehicleId;
		this.accTypeId = accTypeId;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public long getAccTypeId() {
		return accTypeId;
	}
	public void setAccTypeId(long accTypeId) {
		this.accTypeId = accTypeId;
	}
	public long getVehicleId() {
		return vehicleId;
	}
	public void setVehicleId(long vehicleId) {
		this.vehicleId = vehicleId;
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
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof AccAggrBean) {
			AccAggrBean a = (AccAggrBean) obj;
			return this.getVehicleId() == a.getVehicleId() && this.getAccTypeId() == a.getAccTypeId();
		}
		return false;
	}
	public byte[] toByteArray() {
		AccAggrInfo.AccAggr.Builder a = AccAggrInfo.AccAggr.newBuilder();
		a.setID(this.getId());
		a.setAccTypeID(this.getAccTypeId());
		a.setVehicleID(this.getVehicleId());
		a.setStartTime(this.getStartTime());
		a.setEndTime(this.getEndTime());
		a.setCount(this.getCount());
		return a.build().toByteArray();
	}
	
	public AccAggrBean parseFrom(byte[] bytes) throws InvalidProtocolBufferException{
		AccAggrInfo.AccAggr pb = AccAggrInfo.AccAggr.parseFrom(bytes);
		AccAggrBean a = new AccAggrBean(pb.getVehicleID(), pb.getAccTypeID());
		a.setId(pb.getID());
		a.setStartTime(pb.getStartTime());
		a.setEndTime(pb.getEndTime());
		a.setCount(pb.getCount());
		return a;
	}
}

