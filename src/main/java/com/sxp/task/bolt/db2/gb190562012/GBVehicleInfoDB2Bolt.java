package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBVehicleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBVehicleInfoDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBVehicleInfoDB2Bolt.class);
	static String TABLE_NAME = "T_DR_VEHICLE_MESSAGE_RECORD";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_PLATE_CODE", Types.VARCHAR, 32),// 3
			new Column("F_ENTERPRISE_CODE", Types.BIGINT),// 4
			new Column("F_TERMINAL_ID", Types.BIGINT),// 5
			new Column("F_TERMINAL_CODE", Types.VARCHAR, 32),// 6
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 7
			new Column("F_ORDER_ID", Types.BIGINT),// 8
			new Column("F_VEHICLE_ID_CODE", Types.VARCHAR, 17),// 9
			new Column("F_PLATE_TYPE", Types.VARCHAR, 20),// 10
			new Column("F_PLATE_CODES", Types.VARCHAR, 1) // 11
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gbvehicleinfoList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gbvehicleinfoList.size(); i < size; i++) {
			try {
				GBVehicleInfo gbvehicleinfo = GBVehicleInfo.parseFrom(gbvehicleinfoList.get(i));
				s.add(buildSqlObjectMapFromGBVehicleInfo(gbvehicleinfo));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBVehicleInfo error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectMapFromGBVehicleInfo(GBVehicleInfo gbvehicleinfo) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gbvehicleinfo.getID());
		m.put(2, gbvehicleinfo.getVehicleID());
		m.put(3, gbvehicleinfo.getVehicleName());
		m.put(4, gbvehicleinfo.getEnterpriseCode());
		m.put(5, gbvehicleinfo.getTerminalID());
		m.put(6, gbvehicleinfo.getTerminalCode());
		m.put(7, new java.sql.Date(gbvehicleinfo.getRecordTime()));
		m.put(8, gbvehicleinfo.getOrderID());
		m.put(9, gbvehicleinfo.getVehicleCode());
		m.put(10, gbvehicleinfo.getVehicleNType());
		m.put(11, gbvehicleinfo.getVehicleN());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}