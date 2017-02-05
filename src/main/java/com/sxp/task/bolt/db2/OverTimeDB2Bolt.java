package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.OverTimeInfo.OverTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OverTimeDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(OverTimeDB2Bolt.class);
	static String TABLE_NAME = "T_APPEND_DRIVE_TIME_HISTORY";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_LOCATION_ID", Types.BIGINT),// 2
			new Column("F_APPEND_ID", Types.INTEGER),// 3
			new Column("F_ROAD_ID", Types.INTEGER),// 4
			new Column("F_DRIVE_TIME", Types.INTEGER),// 5
			new Column("F_RESULT", Types.SMALLINT),// 6
			new Column("F_TIME", Types.TIMESTAMP),// 7
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 8
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> overtimeList) throws InvalidProtocolBufferException {
		if(overtimeList.size() == 0)
			return null;
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = overtimeList.size(); i < size; i++) {
			try {
				OverTime overtime = OverTime.parseFrom(overtimeList.get(i));
				s.add(buildSqlObjectMapFromOverTime(overtime));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse OverTime error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectMapFromOverTime(OverTime overtime) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, overtime.getID());
		m.put(2, overtime.getLocationID());
		m.put(3, overtime.getAppendID());
		m.put(4, overtime.hasRoadID() ? overtime.getRoadID() : null);
		m.put(5, overtime.getDriveTime());
		m.put(6, overtime.getResult() ? 1 : 0);
		m.put(7, new java.sql.Date(overtime.getGpsTime()));
		m.put(8, overtime.getEnterpriseCode());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}