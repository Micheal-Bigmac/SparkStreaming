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
import com.hsae.hbase.table.History.OilQ;
import com.sxp.task.protobuf.generated.OilMassInfo.OilMass;
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
public class OilMassMapper extends HbaseMapper {
	private static final Logger LOG = LoggerFactory.getLogger(OilMassMapper.class);

	@Override
	public List<Mutation> mutations(Tuple tuple) {
		List<byte[]> bytesList = (List<byte[]>) tuple.getValue(0);
		List<Mutation> mutations = new ArrayList<Mutation>();
		for (byte[] bytes : bytesList) {
			try {
				OilMass oil = OilMass.parseFrom(bytes);
				Put p = new Put(History.buildRowKeyUsingReverseVehicleIdAndBCDDateTime(oil.getVehicleID(), oil.getGpsTime()));
				p.addColumn(History.CF_OIL, OilQ.FLAG, Bytes.toBytes((short) oil.getFlag()));
				p.addColumn(History.CF_OIL, OilQ.LIQUID_LEVEL, Bytes.toBytes(oil.getLiquidLevel()));
				p.addColumn(History.CF_OIL, OilQ.FUEL_CHARGE, Bytes.toBytes(oil.getFuelCharge()));
				p.addColumn(History.CF_OIL, OilQ.OILMASS_VALUE, Bytes.toBytes(oil.getOilMassValue()));
				p.addColumn(History.CF_OIL, OilQ.DECREASE, Bytes.toBytes(oil.getDecrease()));
				p.addColumn(History.CF_OIL, OilQ.LIQUID_LEVEL_INCREASE, Bytes.toBytes(oil.getLiquidLevelIncrease()));
				p.addColumn(History.CF_OIL, OilQ.LIQUID_LEVEL_DECREASE, Bytes.toBytes(oil.getLiquidLevelDecrease()));
				p.addColumn(History.CF_OIL, OilQ.IS_AGG, Bytes.toBytes(oil.getIsAgg()));
				p.addColumn(History.CF_OIL, OilQ.PROTOCOLVER, Bytes.toBytes(oil.getProtocolVer()));
				mutations.add(p);
			} catch (InvalidProtocolBufferException e) {
				LOG.error("protobuf parse oil error! ", e);
			}
		}
		return mutations;
	}
}
