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
import com.hsae.hbase.table.History.GpsQ;
import com.sxp.task.protobuf.generated.AppendInfo.Append;
import com.sxp.task.protobuf.generated.AppendInfo.AppendItem;
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
public class GpsAppendMapper extends HbaseMapper {
	private static final Logger LOG = LoggerFactory.getLogger(GpsAppendMapper.class);

	@Override
	public List<Mutation> mutations(Tuple tuple) {
		List<byte[]> bytesList = (List<byte[]>) tuple.getValue(0);
		List<Mutation> mutations = new ArrayList<Mutation>();
		for (int i = 0, size = bytesList.size(); i < size; i++) {
			byte[] bytes = bytesList.get(i);
			try {
				Append gps = Append.parseFrom(bytes);
				Put p = new Put(History.buildRowKeyUsingReverseVehicleIdAndBCDDateTime(gps.getVehicleID(), gps.getGpsTime()));
				List<AppendItem> itemsList = gps.getItemsList();
				for (AppendItem appendItem : itemsList) {
					p.addColumn(History.CF_GPS, GpsQ.getAppendQ(appendItem.getAppendID()), Bytes.toBytes(appendItem.getAppendValue()));
				}
				mutations.add(p);
			} catch (InvalidProtocolBufferException e) {
				LOG.error("protobuf parse gps error! ", e);
			}
		}
		return mutations;
	}
}
