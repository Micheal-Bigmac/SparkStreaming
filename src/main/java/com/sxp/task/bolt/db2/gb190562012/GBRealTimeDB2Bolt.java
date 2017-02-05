package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBRealTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBRealTimeDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBRealTimeDB2Bolt.class);
	static String TABLE_NAME = "T_DR_CURRENT_TIME";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_PLATE_CODE", Types.VARCHAR, 32),// 3
			new Column("F_ENTERPRISE_CODE", Types.BIGINT),// 4
			new Column("F_TERMINAL_ID", Types.BIGINT),// 5
			new Column("F_TERMINAL_CODE", Types.VARCHAR, 32),// 6
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 7
			new Column("F_ORDER_ID", Types.BIGINT),// 8
			new Column("F_CURRENT_TIME", Types.TIMESTAMP) // 9
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gbrealtimeList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gbrealtimeList.size(); i < size; i++) {
			try {
				GBRealTime gbrealtime = GBRealTime.parseFrom(gbrealtimeList.get(i));
				s.add(buildSqlObjectMapFromGBRealTime(gbrealtime));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBRealTime error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectMapFromGBRealTime(GBRealTime gbrealtime) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gbrealtime.getID());
		m.put(2, gbrealtime.getVehicleID());
		m.put(3, gbrealtime.getVehicleName());
		m.put(4, gbrealtime.getEnterpriseCode());
		m.put(5, gbrealtime.getTerminalID());
		m.put(6, gbrealtime.getTerminalCode());
		m.put(7, new java.sql.Date(gbrealtime.getRecordTime()));
		m.put(8, gbrealtime.getOrderID());
		m.put(9, new java.sql.Date(gbrealtime.getRealTime()));
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}