package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.OverSpeedInfo.OverSpeed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OverSpeedDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(OverSpeedDB2Bolt.class);
	static String TABLE_NAME = "T_OVERSPEED_";
	static String TABLE_ERR_NAME = "T_OVERSPEED_ERR";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT), // 1
			new Column("F_LOCATION_ID", Types.BIGINT), // 2
			new Column("F_APPEND_ID", Types.INTEGER), // 3
			new Column("F_LOCATION_TYPE", Types.SMALLINT), // 4
			new Column("F_AREA_ID", Types.INTEGER), // 5
			new Column("F_TIME", Types.TIMESTAMP), // 6
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 7
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> overspeedList)
			throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = null;
		String INSERT_PREPARED_SQL = null;
		Map<String, List<Map<Integer, Object>>> tableListMap = new HashMap<String, List<Map<Integer, Object>>>();
		for (int i = 0, size = overspeedList.size(); i < size; i++) {
			try {
				OverSpeed overspeed = OverSpeed.parseFrom(overspeedList.get(i));
				if (DateUtil.subtractOneDay() <= overspeed.getGpsTime() && DateUtil.addOneDay() >= overspeed.getGpsTime()) {
					String gpsTime = DateUtil.getStrTime(overspeed.getGpsTime(), "yyyyMMdd");
					String TABLE_NAME_DATE = TABLE_NAME + gpsTime;
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME_DATE, COLUMNS);
				} else {
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERR_NAME, COLUMNS);
				}
				if (!tableListMap.containsKey(INSERT_PREPARED_SQL)) {
					s = new ArrayList<Map<Integer, Object>>();
					s.add(buildSqlObjectListFromOverSpeed(overspeed));
					tableListMap.put(INSERT_PREPARED_SQL, s);
				} else {
					tableListMap.get(INSERT_PREPARED_SQL).add(buildSqlObjectListFromOverSpeed(overspeed));
				}
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse OverSpeed error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		for (String key : tableListMap.keySet()) {
			preparedSqlAndValuesList.add(new Insert(key, COLUMNS, tableListMap.get(key)));
		}
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromOverSpeed(OverSpeed overspeed) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, overspeed.getID());
		m.put(2, overspeed.getLocationID());
		m.put(3, overspeed.getAppendID());
		m.put(4, overspeed.getLocationType());
		m.put(5, overspeed.hasAreaID() ? overspeed.getAreaID() : null);
		m.put(6, new java.sql.Date(overspeed.getGpsTime()));
		m.put(7, overspeed.getEnterpriseCode());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> overspeedList)
			throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERR_NAME, COLUMNS);
		for (int i = 0, size = overspeedList.size(); i < size; i++) {
			try {
				OverSpeed overspeed = OverSpeed.parseFrom(overspeedList.get(i));
				s.add(buildSqlObjectListFromOverSpeed(overspeed));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse OverSpeed error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}
}