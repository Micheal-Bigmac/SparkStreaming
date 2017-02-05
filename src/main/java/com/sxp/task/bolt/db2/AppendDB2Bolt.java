package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.AppendInfo.Append;
import com.sxp.task.protobuf.generated.AppendInfo.AppendItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppendDB2Bolt extends AbstractDB2Bolt {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(AppendDB2Bolt.class);
	static String TABLE_NAME = "T_APPEND_";
	static String TABLE_ERROR_NAME = "T_APPEND_ERR";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT), // 1
			new Column("F_LOCATION_ID", Types.BIGINT), // 2
			new Column("F_APPEND_ID", Types.INTEGER), // 3
			new Column("F_APPEND_VALUE", Types.VARCHAR, (50)), // 4
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 5
	};

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> appendList)
			throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = null;
		Map<String, List<Map<Integer, Object>>> tableListMap = new HashMap<String, List<Map<Integer, Object>>>();
		String INSERT_PREPARED_SQL = NULL;
		
		for (int i = 0, size = appendList.size(); i < size; i++) {
			try {
				Append append = Append.parseFrom(appendList.get(i));
				if (DateUtil.subtractOneDay() <= append.getGpsTime() && DateUtil.addOneDay() >= append.getGpsTime()){
					String gpsTime = DateUtil.getStrTime(append.getGpsTime(), "yyyyMMdd");
					String TABLE_NAME_DATE = TABLE_NAME + gpsTime;
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME_DATE, COLUMNS);
				}else{
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERROR_NAME, COLUMNS);
				}
				if (!tableListMap.containsKey(INSERT_PREPARED_SQL)) {
					s = new ArrayList<Map<Integer, Object>>();
					s.addAll(buildSqlObjectListFromAppend(append));
					tableListMap.put(INSERT_PREPARED_SQL, s);
				} else {
					tableListMap.get(INSERT_PREPARED_SQL).addAll(buildSqlObjectListFromAppend(append));
				}
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse Append error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		for (String key : tableListMap.keySet()) {
			preparedSqlAndValuesList.add(new Insert(key, COLUMNS, tableListMap.get(key)));
		}
		return preparedSqlAndValuesList;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromAppend(Append append) {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<AppendItem> items = append.getItemsList();
		for (AppendItem appendItem : items) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, appendItem.getID());
			m.put(2, append.getLocationID());
			m.put(3, appendItem.getAppendID());
			m.put(4, appendItem.getAppendValue());
			m.put(5, append.getEnterpriseCode());
			s.add(m);
		}
		return s;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> appendList)
			throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> sqlObjectMapList = new ArrayList<Map<Integer, Object>>();
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERROR_NAME, COLUMNS);
		for (int i = 0, size = appendList.size(); i < size; i++) {
			try {
				Append append = Append.parseFrom(appendList.get(i));
				sqlObjectMapList.addAll(buildSqlObjectListFromAppend(append));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse Append error", e);
			}
		}
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, sqlObjectMapList));
		return preparedSqlAndValuesList;
	}
}