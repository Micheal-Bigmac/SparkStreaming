/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sxp.task.bolt.hbase.mapper;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.table.History;
import com.hsae.hbase.table.History.AlarmAggrQ;
import com.sxp.task.protobuf.generated.AlarmAggrInfo2.OverSpeed;
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
public class OverSpeedAlarmMapper extends HbaseMapper {
	private static final Logger LOG = LoggerFactory.getLogger(OverSpeedAlarmMapper.class);

	@Override
	public List<Mutation> mutations(Tuple tuple) {
		List<byte[]> bytesList = (List<byte[]>) tuple.getValue(0);
		List<Mutation> mutations = new ArrayList<Mutation>();
		for (byte[] bytes : bytesList) {
			try {
				OverSpeed alarm = OverSpeed.parseFrom(bytes);
				LOG.debug(alarm.toString());
				long alarmTypeID = alarm.getAlarmTypeId();
				Put p = new Put(History.buildRowKeyUsingReverseVehicleIdAndBCDDateTime(alarm.getVehicleId(), alarm.getStartTime()));
				p.addColumn(History.CF_ALARM_AGGR, AlarmAggrQ.getAggrQ((int) alarmTypeID, AlarmAggrQ.WHETHER), new byte[] { 0x01 });
				p.addColumn(History.CF_ALARM_AGGR, AlarmAggrQ.getAggrQ((int) alarmTypeID, AlarmAggrQ.AGGR_END_TIME), Bytes.toBytes(alarm.getEndTime()));
				p.addColumn(History.CF_ALARM_AGGR, AlarmAggrQ.getAggrQ((int) alarmTypeID, AlarmAggrQ.AGGR_DURATION), Bytes.toBytes(alarm.getDuration()));
				p.addColumn(History.CF_ALARM_AGGR, AlarmAggrQ.getAggrQ((int) alarmTypeID, AlarmAggrQ.AGGR_COUNT), Bytes.toBytes(alarm.getCount()));
				p.addColumn(History.CF_ALARM_AGGR, AlarmAggrQ.getAggrQ((int) alarmTypeID, AlarmAggrQ.AGGR_AVERAGE_SPEED), Bytes.toBytes((float) alarm.getAvgSpeed()));
				if (alarm.hasAvgDSpeed()) {
					p.addColumn(History.CF_ALARM_AGGR, AlarmAggrQ.getAggrQ((int) alarmTypeID, AlarmAggrQ.AGGR_AVERAGE_RECORD_SPEED), Bytes.toBytes((float) alarm.getAvgDSpeed()));
				}
				p.addColumn(History.CF_ALARM_AGGR, AlarmAggrQ.getAggrQ(alarmTypeID, AlarmAggrQ.AREA_ID), Bytes.toBytes(alarm.getAreaId()));
				p.addColumn(History.CF_ALARM_AGGR, AlarmAggrQ.getAggrQ(alarmTypeID, AlarmAggrQ.AREA_TYPE), new byte[] { (byte) (alarm.getLocationType() & 0xFF) });
				mutations.add(p);
			} catch (InvalidProtocolBufferException e) {
				LOG.error("protobuf parse over speed alarm error! ", e);
			}
		}
		return mutations;
	}
}
