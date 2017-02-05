package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.GpsInfo.AlarmItem;
import com.sxp.task.protobuf.generated.GpsInfo.Gps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlarmDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(AlarmDB2Bolt.class);
	static String TABLE_NAME = "T_ALARM_";
	static String TABLE_ERROR_NAME = "T_ALARM_ERR";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT), // 1
			new Column("F_LOCATION_ID", Types.BIGINT), // 2
			new Column("F_ALARM_ID", Types.BIGINT), // 3
			new Column("F_VEHICLE_ID", Types.BIGINT), // 4
			new Column("F_TERMINAL_ID", Types.BIGINT), // 5
			new Column("F_TIME", Types.TIMESTAMP), // 6
			new Column("F_PLATE_CODE", Types.VARCHAR, 20), // 7
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 8
	};

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = null;
		Map<String, List<Map<Integer, Object>>> tableListMap = new HashMap<String, List<Map<Integer, Object>>>();

		String INSERT_PREPARED_SQL = NULL;
		for (int i = 0, size = gpsList.size(); i < size; i++) {
			try {
				Gps gps = Gps.parseFrom(gpsList.get(i));
				if (DateUtil.subtractOneDay() <= gps.getGpsDate() && DateUtil.addOneDay() >= gps.getGpsDate()) {
					String gpsTime = DateUtil.getStrTime(gps.getGpsDate(), "yyyyMMdd");
					String TABLE_NAME_DATE = TABLE_NAME + gpsTime;
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME_DATE, COLUMNS);
				}else{
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERROR_NAME, COLUMNS);
				}
				if (!tableListMap.containsKey(INSERT_PREPARED_SQL)) {
					s = new ArrayList<Map<Integer, Object>>();
					s.addAll(buildSqlObjectMapListFromAlarm(gps));
					tableListMap.put(INSERT_PREPARED_SQL, s);
				} else {
					tableListMap.get(INSERT_PREPARED_SQL).addAll(buildSqlObjectMapListFromAlarm(gps));
				}
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse Alarm error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		for (String key : tableListMap.keySet()) {
			preparedSqlAndValuesList.add(new Insert(key, COLUMNS, tableListMap.get(key)));
		}
		return preparedSqlAndValuesList;
	}
	private static List<Map<Integer, Object>> buildSqlObjectMapListFromAlarm(Gps gps) {

		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<AlarmItem> items = gps.getAlarmItemsList();
		for (AlarmItem alarmItem : items) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, alarmItem.getAlarmID());
			m.put(2, gps.getLocationID());
			m.put(3, alarmItem.getAlarmType());
			m.put(4, gps.getVehicleID());
			m.put(5, gps.getTerminalID());
			m.put(6, new java.sql.Date(gps.getGpsDate()));
			m.put(7, "");
			m.put(8, gps.getEnterpriseCode());
			s.add(m);
		}
		return s;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> alarmList)
			throws InvalidProtocolBufferException {
		return null;
	}
}