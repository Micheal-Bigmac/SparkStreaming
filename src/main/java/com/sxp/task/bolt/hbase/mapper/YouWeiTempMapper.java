package com.sxp.task.bolt.hbase.mapper;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.table.History;
import com.hsae.hbase.table.History.GpsQ;
import com.sxp.task.protobuf.generated.YouWei;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class YouWeiTempMapper extends HbaseMapper {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(YouWeiTempMapper.class);

	@Override
	public List<Mutation> mutations(Tuple tuple) {
		List<byte[]> bytesList = (List<byte[]>) tuple.getValue(0);
		List<Mutation> mutations = new ArrayList<Mutation>();
		for (int i = 0, size = bytesList.size(); i < size; i++) {
			byte[] bytes = bytesList.get(i);
			try {
				YouWei.YouWeiTemp t = YouWei.YouWeiTemp.parseFrom(bytes);
				Put p = new Put(History.buildRowKeyUsingReverseVehicleIdAndBCDDateTime(t.getVehicleID(), t.getGpsTime()));
				List<YouWei.YouWeiTempD> itemsList = t.getItemsList();
				for (YouWei.YouWeiTempD item : itemsList) {
					p.addColumn(History.CF_TEMPERATURE, GpsQ.getAppendQ(item.getTempID()), Bytes.toBytes(item.getTempValue()));
				}
				mutations.add(p);
			} catch (InvalidProtocolBufferException e) {
				LOG.error("protobuf parse youwei temp error! ", e);
			}
		}
		return mutations;
	}
}
