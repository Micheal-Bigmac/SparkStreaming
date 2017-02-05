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
import com.hsae.hbase.table.History.TriaxialQ;
import com.sxp.task.protobuf.generated.TriaxiaDataInfo.TriaxialData;
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
public class TriaxialMapper extends HbaseMapper {
	private static final Logger LOG = LoggerFactory.getLogger(TriaxialMapper.class);

	@Override
	public List<Mutation> mutations(Tuple tuple) {
		List<byte[]> bytesList = (List<byte[]>) tuple.getValue(0);
		List<Mutation> mutations = new ArrayList<Mutation>();
		for (byte[] bytes : bytesList) {
			try {
				TriaxialData can = TriaxialData.parseFrom(bytes);
				Put p = new Put(History.buildRowKeyUsingReverseVehicleIdAndBCDDateTime(can.getVehicleID(), can.getGpsTime()));
				p.addColumn(History.CF_TRIAXIAL, TriaxialQ.XANGLE, Bytes.toBytes((short) can.getXAngle()));
				p.addColumn(History.CF_TRIAXIAL, TriaxialQ.YANGLE, Bytes.toBytes((short) can.getYAngle()));
				p.addColumn(History.CF_TRIAXIAL, TriaxialQ.ZANGLE, Bytes.toBytes((short) can.getZAngle()));
				p.addColumn(History.CF_TRIAXIAL, TriaxialQ.XACCELERATION, Bytes.toBytes(can.getXAcceleration()));
				p.addColumn(History.CF_TRIAXIAL, TriaxialQ.YACCELERATION, Bytes.toBytes(can.getYAcceleration()));
				p.addColumn(History.CF_TRIAXIAL, TriaxialQ.ZACCELERATION, Bytes.toBytes(can.getZAcceleration()));
				p.addColumn(History.CF_TRIAXIAL, TriaxialQ.TEMPERATURE, Bytes.toBytes((short) can.getTemperature()));
				mutations.add(p);
			} catch (InvalidProtocolBufferException e) {
				LOG.error("protobuf parse triaxial error! ", e);
			}
		}
		return mutations;
	}
}
