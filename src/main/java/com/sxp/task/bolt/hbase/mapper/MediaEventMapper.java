package com.sxp.task.bolt.hbase.mapper;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.table.History;
import com.hsae.hbase.table.History.MediaQ;
import com.sxp.task.protobuf.generated.MediaInfo.MediaEvent;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MediaEventMapper extends HbaseMapper {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(MediaEventMapper.class);

	@Override
	public List<Mutation> mutations(Tuple tuple) {
		List<byte[]> bytesList = (List<byte[]>) tuple.getValue(0);
		List<Mutation> mutations = new ArrayList<Mutation>();
		for (int i = 0, size = bytesList.size(); i < size; i++) {
			byte[] bytes = bytesList.get(i);
			try {
				MediaEvent m = MediaEvent.parseFrom(bytes);
				Put p = new Put(History.buildRowKeyUsingReverseVehicleIdAndBCDDateTime(m.getVehicleID(), m.getEventTime()));
				p.addColumn(History.CF_GPS, MediaQ.ID, Bytes.toBytes(m.getID()));
				p.addColumn(History.CF_GPS, MediaQ.EVENT_ID, Bytes.toBytes(m.getEventID()));
				p.addColumn(History.CF_GPS, MediaQ.MEDIA_TYPE, new byte[] { Bytes.toBytes(m.getMediaType())[0] });
				p.addColumn(History.CF_GPS, MediaQ.MEDIA_ENCODING, new byte[] { Bytes.toBytes(m.getMediaEncoding())[0] });
				p.addColumn(History.CF_GPS, MediaQ.EVENT_TYPE, new byte[] { Bytes.toBytes(m.getEventType())[0] });
				p.addColumn(History.CF_GPS, MediaQ.CHANNEL, new byte[] { Bytes.toBytes(m.getChannel())[0] });
				p.addColumn(History.CF_GPS, MediaQ.LAST_UPDATE, Bytes.toBytes(m.getUpLoadTime()));
				mutations.add(p);
			} catch (InvalidProtocolBufferException e) {
				LOG.error("protobuf parse Media event error! ", e);
			}
		}
		return mutations;
	}
}
