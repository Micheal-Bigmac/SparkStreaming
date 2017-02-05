package com.sxp.task.bolt.hbase.mapper;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.table.History;
import com.hsae.hbase.table.History.AreaInOutQ;
import com.sxp.task.protobuf.generated.AreaInoutBeanInfo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName AreaInOutMapper
 * @Description 
 * @author 熊尧
 * @company 上海势航网络科技有限公司
 * @date 2016年6月30日
 */
public class AreaInOutMapper extends HbaseMapper {

	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG = LoggerFactory.getLogger(AreaInOutMapper.class);
	
	public List<Mutation> mutations(Tuple tuple) {
		@SuppressWarnings("unchecked")
		List<byte[]> bytesList = (List<byte[]>) tuple.getValue(0);
		List<Mutation> mutations = new ArrayList<Mutation>();
		for (byte[] bytes : bytesList) {
			try {
				AreaInoutBeanInfo.AreaInoutBean areaInoutBean = AreaInoutBeanInfo.AreaInoutBean.parseFrom(bytes);
				Put p = new Put(History.buildRowKeyUsingReverseVehicleIdAndBCDDateTime(areaInoutBean.getVehicleId(), areaInoutBean.getStartTime()));
				p.addColumn(History.CF_AREAINOUT_AGGR, Bytes.add(AreaInOutQ.AVG_DSPEED, Bytes.toBytes(areaInoutBean.getAreaId())),Bytes.toBytes(areaInoutBean.getAvgDSpeed()));
				p.addColumn(History.CF_AREAINOUT_AGGR, Bytes.add(AreaInOutQ.AVG_SPEED, Bytes.toBytes(areaInoutBean.getAreaId())),Bytes.toBytes(areaInoutBean.getAvgSpeed()));
				p.addColumn(History.CF_AREAINOUT_AGGR, Bytes.add(AreaInOutQ.DURATION, Bytes.toBytes(areaInoutBean.getAreaId())),Bytes.toBytes(areaInoutBean.getDuration()));
				p.addColumn(History.CF_AREAINOUT_AGGR, Bytes.add(AreaInOutQ.START_LOCATION_ID, Bytes.toBytes(areaInoutBean.getAreaId())),Bytes.toBytes(areaInoutBean.getStartLocationId()));
				p.addColumn(History.CF_AREAINOUT_AGGR, Bytes.add(AreaInOutQ.END_LOCATION_ID, Bytes.toBytes(areaInoutBean.getAreaId())),Bytes.toBytes(areaInoutBean.getEndLocationId()));
				p.addColumn(History.CF_AREAINOUT_AGGR, Bytes.add(AreaInOutQ.START_TIME, Bytes.toBytes(areaInoutBean.getAreaId())),Bytes.toBytes(areaInoutBean.getStartTime()));
				p.addColumn(History.CF_AREAINOUT_AGGR, Bytes.add(AreaInOutQ.END_TIME, Bytes.toBytes(areaInoutBean.getAreaId())),Bytes.toBytes(areaInoutBean.getEndTime()));
				p.addColumn(History.CF_AREAINOUT_AGGR, Bytes.add(AreaInOutQ.LOCATION_TYPE, Bytes.toBytes(areaInoutBean.getAreaId())),Bytes.toBytes(areaInoutBean.getLocationType()));
				p.addColumn(History.CF_AREAINOUT_AGGR, Bytes.add(AreaInOutQ.MAX_DSPEED, Bytes.toBytes(areaInoutBean.getAreaId())),Bytes.toBytes(areaInoutBean.getMaxDSpeed()));
				p.addColumn(History.CF_AREAINOUT_AGGR, Bytes.add(AreaInOutQ.MAX_SPEED, Bytes.toBytes(areaInoutBean.getAreaId())),Bytes.toBytes(areaInoutBean.getMaxSpeed()));
				p.addColumn(History.CF_AREAINOUT_AGGR, Bytes.add(AreaInOutQ.MIN_DSPEED, Bytes.toBytes(areaInoutBean.getAreaId())),Bytes.toBytes(areaInoutBean.getMinDSpeed()));
				p.addColumn(History.CF_AREAINOUT_AGGR, Bytes.add(AreaInOutQ.MIN_SPEED, Bytes.toBytes(areaInoutBean.getAreaId())),Bytes.toBytes(areaInoutBean.getMinSpeed()));
				mutations.add(p);
			} catch (InvalidProtocolBufferException e) {
				LOG.error("protobuf parse enter or exit area or line error! ", e);
			}
		}
		return mutations;
	}
	

}

