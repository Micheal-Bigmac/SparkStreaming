package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.AlarmAggrInfo.AlarmAggr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlarmAggrDB2Bolt extends AbstractDB2Bolt {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(AlarmAggrDB2Bolt.class);
	static String TABLE_NAME = "T_ALARM_AGGR_";
	static String TABLE_ERROR_NAME = "T_ALARM_AGGR_ERR";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT), // 1
			new Column("F_ALARM_ID", Types.BIGINT), // 2
			new Column("F_VEHICLE_ID", Types.BIGINT), // 3
			new Column("F_PLATE_CODE", Types.VARCHAR, 20), // 4
			new Column("F_START_TIME", Types.TIMESTAMP), // 5
			new Column("F_END_TIME", Types.TIMESTAMP), // 6
			new Column("F_COUNT", Types.INTEGER), // 7
			new Column("F_STATUS", Types.SMALLINT), // 8
			new Column("F_USER_ID", Types.BIGINT), // 9
			new Column("F_CONTEXT", Types.VARCHAR), // 10
			new Column("F_START_ID", Types.BIGINT), // 11
			new Column("F_END_ID", Types.BIGINT), // 12
			new Column("F_UPDATE_TIME", Types.TIMESTAMP), // 13
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32), // 14
			new Column("F_SPEEDINGVALUE", Types.DOUBLE), // 15
			new Column("F_ALARMBAK", Types.VARCHAR, 200), // 16
			new Column("F_RULESID", Types.INTEGER), // 17
			new Column("F_SPEED", Types.SMALLINT), // 18
			new Column("F_DSPEED", Types.SMALLINT) // 19
	};

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> alarmaggrList)
			throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> sqlObjectMapList = new ArrayList<Map<Integer, Object>>();
		Map<String, List<Map<Integer, Object>>> tableListMap = new HashMap<String, List<Map<Integer, Object>>>();
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		String INSERT_PREPARED_SQL = NULL;
		for (int i = 0, size = alarmaggrList.size(); i < size; i++) {
			try {
				AlarmAggr alarmaggr = AlarmAggr.parseFrom(alarmaggrList.get(i));
				if (DateUtil.subtractOneDay() <= alarmaggr.getStartTime() && DateUtil.addOneDay() >= alarmaggr.getStartTime()){
					String gpsTime = DateUtil.getStrTime(alarmaggr.getStartTime(), "yyyyMMdd");
					String TABLE_NAME_DATE = TABLE_NAME + gpsTime;
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME_DATE, COLUMNS);
				}else{
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERROR_NAME, COLUMNS);
				}
				if (!tableListMap.containsKey(INSERT_PREPARED_SQL)) {
					sqlObjectMapList.add(buildSqlObjectListFromAlarmAggr(alarmaggr));
					tableListMap.put(INSERT_PREPARED_SQL, sqlObjectMapList);
				} else {
					tableListMap.get(INSERT_PREPARED_SQL).add(buildSqlObjectListFromAlarmAggr(alarmaggr));
				}
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse AlarmAggr error", e);
			}
		}
		for (String key : tableListMap.keySet()) {
			preparedSqlAndValuesList.add(new Insert(key, COLUMNS, tableListMap.get(key)));
		}
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromAlarmAggr(AlarmAggr alarmaggr) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, alarmaggr.getID());
		m.put(2, alarmaggr.getAlarmTypeID());
		m.put(3, alarmaggr.getVehicleID());
		m.put(4, alarmaggr.getPlateCode());
		m.put(5, new java.sql.Date(alarmaggr.getStartTime()));
		m.put(6, new java.sql.Date(alarmaggr.getEndTime()));
		m.put(7, alarmaggr.getCount());
		m.put(8, alarmaggr.getStatus());
		m.put(9, alarmaggr.hasUserID() ? alarmaggr.getUserID() : null);
		m.put(10, alarmaggr.getContext());
		m.put(11, alarmaggr.getStartID());
		m.put(12, alarmaggr.getEndID());
		m.put(13, new java.sql.Date(alarmaggr.getUpdateTime()));
		m.put(14, alarmaggr.getEnterpriseCode());
		m.put(15, alarmaggr.getSpeedingValue());
		m.put(16, alarmaggr.getAlarmRemark());
		m.put(17, alarmaggr.getRulesID());
		m.put(18, alarmaggr.getSpeed());
		m.put(19, alarmaggr.hasDSpeed() ? alarmaggr.getDSpeed() : null);
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> alarmaggrList)
			throws InvalidProtocolBufferException {

		List<Map<Integer, Object>> sqlObjectMapList = new ArrayList<Map<Integer, Object>>();
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERROR_NAME, COLUMNS);
		for (int i = 0, size = alarmaggrList.size(); i < size; i++) {
			try {
				AlarmAggr alarmaggr = AlarmAggr.parseFrom(alarmaggrList.get(i));
				sqlObjectMapList.add(buildSqlObjectListFromAlarmAggr(alarmaggr));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse AlarmAggr error", e);
			}
		}
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, sqlObjectMapList));
		return preparedSqlAndValuesList;
	}

}