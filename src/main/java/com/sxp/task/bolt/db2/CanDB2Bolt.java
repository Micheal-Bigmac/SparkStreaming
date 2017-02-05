package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.CanInfo.Can;
import com.sxp.task.protobuf.generated.CanInfo.CanItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CanDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(CanDB2Bolt.class);
	static String CAN_TABLE_NAME = "T_CAN_";
	static String TABLE_CAN_ERROR_NAME = "T_CAN_ERR";
	public static Column[] CAN_COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT), // 1
			new Column("F_LOCATION_ID", Types.BIGINT), // 2
			new Column("F_VEHICLE_ID", Types.BIGINT), // 3
			new Column("F_CAN_TIME", Types.TIMESTAMP), // 4
			new Column("F_RECV_TIME", Types.TIMESTAMP), // 5
			new Column("F_TERMINAL_ID", Types.BIGINT), // 6
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 7
	};

	static String CAN_DETAILS_TABLE_NAME = "T_CAN_DETAIL_";
	static String TABLE_CAN_DETAIL_ERROR_NAME = "T_CAN_DETAIL_ERR";
	public static Column[] CAN_DETAILS_COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT), // 1
			new Column("F_CAN_ID", Types.BIGINT), // 2
			new Column("F_CAN_TYPE_ID", Types.BIGINT), // 3
			new Column("F_VALUE", Types.DOUBLE), // 4
			new Column("F_CONTENT", Types.VARCHAR, 64), // 5
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 6
	};

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> canList)
			throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> canSqlObjectMapList = null;
		List<Map<Integer, Object>> canItemsSqlObjectMapList = null;
		Map<String, List<Map<Integer, Object>>> canListMap = new HashMap<String, List<Map<Integer, Object>>>();
		Map<String, List<Map<Integer, Object>>> canDetailListMap = new HashMap<String, List<Map<Integer, Object>>>();
		String CAN_INSERT_PREPARED_SQL = null;
		String CAN_DETAILS_INSERT_PREPARED_SQL = null;
		for (int i = 0, size = canList.size(); i < size; i++) {
			try {
				Can can = Can.parseFrom(canList.get(i));
				if (DateUtil.subtractOneDay() <= can.getCanTime() && DateUtil.addOneDay() >= can.getCanTime()){
					String canTime = DateUtil.getStrTime(can.getCanTime(), "yyyyMMdd");
					String CAN_TABLE_NAME_DATE = CAN_TABLE_NAME + canTime;
					String CAN_DETAILS_TABLE_NAME_DATE = CAN_DETAILS_TABLE_NAME + canTime;
					CAN_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(CAN_TABLE_NAME_DATE, CAN_COLUMNS);
					CAN_DETAILS_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(CAN_DETAILS_TABLE_NAME_DATE,
							CAN_DETAILS_COLUMNS);
				}else{
					CAN_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_CAN_ERROR_NAME, CAN_COLUMNS);
					CAN_DETAILS_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_CAN_DETAIL_ERROR_NAME,
							CAN_DETAILS_COLUMNS);
				}
				// can数据map
				if (!canListMap.containsKey(CAN_INSERT_PREPARED_SQL)) {
					canSqlObjectMapList = new ArrayList<Map<Integer, Object>>();
					canSqlObjectMapList.add(buildSqlObjectListFromCan(can));
					canListMap.put(CAN_INSERT_PREPARED_SQL, canSqlObjectMapList);
				} else {
					canListMap.get(CAN_INSERT_PREPARED_SQL).add(buildSqlObjectListFromCan(can));
				}
				// canDetail数据map
				if (!canDetailListMap.containsKey(CAN_DETAILS_INSERT_PREPARED_SQL)) {
					canItemsSqlObjectMapList = new ArrayList<Map<Integer, Object>>();
					canItemsSqlObjectMapList.addAll(
							buildSqlObjectListFromCanItems(can.getID(), can.getEnterpriseCode(), can.getItemsList()));
					canDetailListMap.put(CAN_DETAILS_INSERT_PREPARED_SQL, canItemsSqlObjectMapList);
				} else {
					canDetailListMap.get(CAN_DETAILS_INSERT_PREPARED_SQL).addAll(
							buildSqlObjectListFromCanItems(can.getID(), can.getEnterpriseCode(), can.getItemsList()));
				}
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse Can error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		for (String key : canListMap.keySet()) {
			preparedSqlAndValuesList.add(new Insert(key, CAN_COLUMNS, canListMap.get(key)));
		}
		for (String key : canDetailListMap.keySet()) {
			preparedSqlAndValuesList.add(new Insert(key, CAN_DETAILS_COLUMNS, canDetailListMap.get(key)));
		}
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromCan(Can can) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, can.getID());
		m.put(2, can.getLocationID());
		m.put(3, can.getVehicleID());
		m.put(4, new java.sql.Date(can.getCanTime()));
		m.put(5, new java.sql.Date(can.getRecvTime()));
		m.put(6, can.getTerminalID());
		m.put(7, can.getEnterpriseCode());
		return m;
	}

	private List<Map<Integer, Object>> buildSqlObjectListFromCanItems(long canId, String enterpriseCode,
			List<CanItem> itemsList) {
		List<Map<Integer, Object>> canItemsSqlObjectMapList = new ArrayList<Map<Integer, Object>>();
		for (CanItem c : itemsList) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, c.getID());
			m.put(2, canId);
			m.put(3, c.getCanTypeID());
			m.put(4, c.hasCanValue() ? c.getCanValue() : null);
			m.put(5, c.hasCanContent() ? c.getCanContent() : null);
			m.put(6, enterpriseCode);
			canItemsSqlObjectMapList.add(m);
		}
		return canItemsSqlObjectMapList;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> canList)
			throws InvalidProtocolBufferException {

		List<Map<Integer, Object>> canSqlObjectMapList = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> canItemsSqlObjectMapList = new ArrayList<Map<Integer, Object>>();
		String CAN_INSERT_PREPARED_SQL = null;
		String CAN_DETAILS_INSERT_PREPARED_SQL = null;
		for (int i = 0, size = canList.size(); i < size; i++) {
			try {
				Can can = Can.parseFrom(canList.get(i));
				String CAN_TABLE_NAME_DATE = TABLE_CAN_ERROR_NAME ;
				String CAN_DETAILS_TABLE_NAME_DATE = TABLE_CAN_DETAIL_ERROR_NAME;
				CAN_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(CAN_TABLE_NAME_DATE, CAN_COLUMNS);
				CAN_DETAILS_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(CAN_DETAILS_TABLE_NAME_DATE,
						CAN_DETAILS_COLUMNS);
				canSqlObjectMapList.add(buildSqlObjectListFromCan(can));
				canItemsSqlObjectMapList.addAll(
						buildSqlObjectListFromCanItems(can.getID(), can.getEnterpriseCode(), can.getItemsList()));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse Can error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(CAN_INSERT_PREPARED_SQL, CAN_COLUMNS, canSqlObjectMapList));
		preparedSqlAndValuesList
				.add(new Insert(CAN_DETAILS_INSERT_PREPARED_SQL, CAN_DETAILS_COLUMNS, canItemsSqlObjectMapList));
		return preparedSqlAndValuesList;
	}
}