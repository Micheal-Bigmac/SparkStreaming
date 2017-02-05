package com.sxp.task.bolt.hbase.mapper;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.table.History;
import com.hsae.hbase.table.Middle;
import com.hsae.hbase.table.Middle.MidGpsQ;
import com.sxp.task.protobuf.generated.CtripAggrInfo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description: 车辆行程聚合
 * @author: 熊尧
 * @company: 上海势航网络技术有限公司
 * @date 2016年3月11日
 */
public class CtripAggrMapper extends HbaseMapper {
	
	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG = LoggerFactory.getLogger(GpsMapper.class);
	
	List<CtripAggrInfo.CtripAggr> ctripAggrList = new ArrayList<CtripAggrInfo.CtripAggr>();
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Mutation> mutations(Tuple tuple){
		
		Long vehicleId = (Long) tuple.getValue(0);
		List<byte[]> bytesList = (List<byte[]>) tuple.getValue(1);
		
		List<Mutation> mutations = new ArrayList<Mutation>();
		 for(byte[] bytes : bytesList){
			try {
				 CtripAggrInfo.CtripAggr ctrip = CtripAggrInfo.CtripAggr.parseFrom(bytes);
				 ctripAggrList.add(ctrip);
			} catch (InvalidProtocolBufferException e) {
				LOG.error("protobuf parse gps error! ", e);
			}
		 }
			
		//写Hbase库
			for (CtripAggrInfo.CtripAggr ctripAgg : ctripAggrList) {
				
				//组装startRowKey,endRowKey
				byte[] startRowkey = Bytes.add(History.buildReverseVehicleid(vehicleId), ctripAgg.getStime().toByteArray());
				byte[] endRowkey = Bytes.add(History.buildReverseVehicleid(vehicleId), ctripAgg.getEtime().toByteArray());
				//截取开始时间的年月日作为rowkey的前缀
				byte[] rowkeyPrefix = Bytes.copy(ctripAgg.getStime().toByteArray(), 0, 3);
				//车辆id反转+rowkeyPrefix作为Middle表，行程聚合的rowkey，按天对行程进行聚合
				Put p = new Put(Bytes.add(Middle.buildReverseVehicleid(vehicleId), rowkeyPrefix));
				p.addColumn(Middle.CF_GPS, Bytes.add(MidGpsQ.DTISANCE_PREFIX, MidGpsQ.CTRIP_STARTROWKEY,Bytes.toBytes(String.valueOf(ctripAgg.getOrderId()))), startRowkey);
				p.addColumn(Middle.CF_GPS, Bytes.add(MidGpsQ.DTISANCE_PREFIX, MidGpsQ.CTRIP_ENDROWKEY,Bytes.toBytes(String.valueOf(ctripAgg.getOrderId()))), endRowkey);
				p.addColumn(Middle.CF_GPS, Bytes.add(MidGpsQ.DTISANCE_PREFIX, MidGpsQ.CTRIP_STARTBCDTIME,Bytes.toBytes(String.valueOf(ctripAgg.getOrderId()))), ctripAgg.getStime().toByteArray());
				p.addColumn(Middle.CF_GPS, Bytes.add(MidGpsQ.DTISANCE_PREFIX, MidGpsQ.CTRIP_ENDBCDTIME,Bytes.toBytes(String.valueOf(ctripAgg.getOrderId()))), ctripAgg.getEtime().toByteArray());
				p.addColumn(Middle.CF_GPS, Bytes.add(MidGpsQ.DTISANCE_PREFIX, MidGpsQ.CTRIP_GPSMILEAGE,Bytes.toBytes(String.valueOf(ctripAgg.getOrderId()))), Bytes.toBytes(ctripAgg.getGpsmile()));
				p.addColumn(Middle.CF_GPS, Bytes.add(MidGpsQ.DTISANCE_PREFIX, MidGpsQ.CTRIP_RSPEED,Bytes.toBytes(String.valueOf(ctripAgg.getOrderId()))), Bytes.toBytes(ctripAgg.getRavgspd()));
				p.addColumn(Middle.CF_GPS, Bytes.add(MidGpsQ.DTISANCE_PREFIX, MidGpsQ.CTRIP_RMILEAGE,Bytes.toBytes(String.valueOf(ctripAgg.getOrderId()))), Bytes.toBytes(ctripAgg.getRmile()));
				p.addColumn(Middle.CF_GPS, Bytes.add(MidGpsQ.DTISANCE_PREFIX, MidGpsQ.CTRIP_GPSAVGSPEED,Bytes.toBytes(String.valueOf(ctripAgg.getOrderId()))), Bytes.toBytes(ctripAgg.getGpsavgspd()));
				p.addColumn(Middle.CF_GPS, Bytes.add(MidGpsQ.DTISANCE_PREFIX, MidGpsQ.CTRIP_ISNEXTDAY,Bytes.toBytes(String.valueOf(ctripAgg.getOrderId()))), Bytes.toBytes(ctripAgg.getIsnextday()));
				mutations.add(p);
			}
		return mutations;
	}
}
