package com.sxp.task.bolt.hbase.mapper;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.table.History;
import com.hsae.hbase.table.History.CanQ;
import com.sxp.task.protobuf.generated.CanInfo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class CanMapper extends HbaseMapper {
	private static final Logger LOG = LoggerFactory.getLogger(CanMapper.class);

	public List<Mutation> mutations(Tuple tuple) {
		List<byte[]> bytesList = (List<byte[]>) tuple.getValue(0);
		List<Mutation> mutations = new ArrayList<Mutation>();
		for (byte[] bytes : bytesList) {
			try {
				CanInfo.Can can = CanInfo.Can.parseFrom(bytes);
				Put p = new Put(History.buildRowKeyUsingReverseVehicleIdAndBCDDateTime(can.getVehicleID(), can.getCanTime()));
				List<CanInfo.CanItem> canItemsList = can.getItemsList();
				for (CanInfo.CanItem canItem : canItemsList) {
					int canTypeID = canItem.getCanTypeID();
					p.addColumn(History.CF_CAN, CanQ.getValueQ(canTypeID), Bytes.toBytes(canItem.getCanValue()));
					if (null != canItem.getCanContent() && !"".equals(canItem.getCanContent())) {
						p.addColumn(History.CF_CAN, CanQ.getValueQ(canTypeID), Bytes.toBytes(canItem.getCanContent()));
					}
					// LOG.warn("received can vehicleid=" + can.getVehicleID() + ", CanTime=" + can.getCanTime() + ", typeid=" + canTypeID + ", CanValue=" + canItem.getCanValue());
				}
				mutations.add(p);
			} catch (InvalidProtocolBufferException e) {
				LOG.error("protobuf parse can error! ", e);
			}
		}
		return mutations;
	}
}
