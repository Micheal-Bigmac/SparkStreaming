package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.AreaInoutInfo.AreaInout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AreaInoutDB2Bolt extends AbstractDB2Bolt {

	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG = LoggerFactory.getLogger(AreaInoutDB2Bolt.class);
	static String TABLE_NAME = "T_INOUTAREA_";
	static String TABLE_ERROR_NAME = "T_INOUTAREA_ERR";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT), // 1
			new Column("F_LOCATION_ID", Types.BIGINT), // 2
			new Column("F_APPEND_ID", Types.INTEGER), // 3
			new Column("F_LOCATION_TYPE", Types.SMALLINT), // 4
			new Column("F_AREA_ID", Types.INTEGER), // 5
			new Column("F_DIRECTION", Types.SMALLINT), // 6
			new Column("F_TIME", Types.TIMESTAMP), // 7
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 8
	};

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> areainoutList)
			throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = null;
		Map<String, List<Map<Integer, Object>>> tableListMap = new HashMap<String, List<Map<Integer, Object>>>();
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		String INSERT_PREPARED_SQL = NULL;
		for (int i = 0, size = areainoutList.size(); i < size; i++) {
			try {
				AreaInout areainout = AreaInout.parseFrom(areainoutList.get(i));
				if (DateUtil.subtractOneDay() <= areainout.getGpsTime() && DateUtil.addOneDay() >= areainout.getGpsTime()){
					String gpsTime = DateUtil.getStrTime(areainout.getGpsTime(), "yyyyMMdd");
					String TABLE_NAME_DATE = TABLE_NAME + gpsTime;
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME_DATE, COLUMNS);
				}else{
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERROR_NAME, COLUMNS);
				}
				if (!tableListMap.containsKey(INSERT_PREPARED_SQL)) {
					s = new ArrayList<Map<Integer, Object>>();
					s.add(buildSqlObjectListFromAreaInout(areainout));
					tableListMap.put(INSERT_PREPARED_SQL, s);
				} else {
					tableListMap.get(INSERT_PREPARED_SQL).add(buildSqlObjectListFromAreaInout(areainout));
				}
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse AreaInout error", e);
			}
		}
		for (String key : tableListMap.keySet()) {
			preparedSqlAndValuesList.add(new Insert(key, COLUMNS, tableListMap.get(key)));
		}
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromAreaInout(AreaInout areainout) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, areainout.getID());
		m.put(2, areainout.getLocationID());
		m.put(3, areainout.getAppendID());
		m.put(4, areainout.getLocationType());
		m.put(5, areainout.getAreaID());
		m.put(6, areainout.getDirection() ? 1 : 0);
		m.put(7, new java.sql.Date(areainout.getGpsTime()));
		m.put(8, areainout.getEnterpriseCode());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> areainoutList)
			throws InvalidProtocolBufferException {
		
		List<Map<Integer, Object>> sqlObjectMapList = new ArrayList<Map<Integer, Object>>();
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERROR_NAME, COLUMNS);
		for (int i = 0, size = areainoutList.size(); i < size; i++) {
			try {
				AreaInout areainout = AreaInout.parseFrom(areainoutList.get(i));
				sqlObjectMapList.add(buildSqlObjectListFromAreaInout(areainout));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse AreaInout error", e);
			}
		}
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, sqlObjectMapList));
		return preparedSqlAndValuesList;
	}
}