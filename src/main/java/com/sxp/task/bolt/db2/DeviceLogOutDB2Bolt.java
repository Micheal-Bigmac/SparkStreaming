package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.Update;
import com.sxp.task.protobuf.generated.DeviceLogOutInfo.DeviceLogOut;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeviceLogOutDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(DeviceLogOutDB2Bolt.class);
	public static final String TABLE_NAME_1 = "T_TERMINAL_REGISTER";
	public static final Column WHERE_COLUMNS_1 = new Column("F_VEHICLE_ID", Types.BIGINT);
	public static final Column[] UPDATE_COLUMNSS_1 = new Column[] { new Column("F_LOGOUT_DATE", Types.TIMESTAMP),//
			new Column("F_IS_LOGOUT", Types.SMALLINT),//
			new Column("F_IP_ADDRESS", Types.VARCHAR, (50)),//
			new Column("F_PORT", Types.INTEGER),//
			new Column("F_UPDATE_DATE", Types.TIMESTAMP),//
			new Column("F_ONLINE", Types.SMALLINT) //
	};

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> list) throws InvalidProtocolBufferException {
		Map<Object, Object[]> where2UpdateValueMap1 = new HashMap<Object, Object[]>();
		Map<Object, Object[]> where2UpdateValueMap2 = new HashMap<Object, Object[]>();
		long vehicleId = 0;
		byte[] vehicle = new byte[8];
		byte[] data = null;
		for (int i = 0, size = list.size(); i < size; i++) {
			try {
				 data = list.get(i);
				System.arraycopy(data, 4, vehicle, 0, 8);
				vehicleId = Bytes.toLong(vehicle);
				byte[] dataContent = new byte[data.length - 12];
				System.arraycopy(data, 12, dataContent, 0, data.length - 12);
				DeviceLogOut d = DeviceLogOut.parseFrom(dataContent);
				where2UpdateValueMap1.put(vehicleId,
						new Object[] { new java.sql.Date(d.getLogoutTime()), 1, d.getIpAddress(), d.getPort(), new java.sql.Date(d.getUpdateTime()), 0 });
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse DeviceLogOut error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Update(TABLE_NAME_1, WHERE_COLUMNS_1, UPDATE_COLUMNSS_1, where2UpdateValueMap1));
		return preparedSqlAndValuesList;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}