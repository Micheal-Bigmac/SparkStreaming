package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.OilMassInfo.OilMass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OilMassDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(OilMassDB2Bolt.class);
	static String TABLE_NAME = "T_OILMASS_";
	static String TABLE_ERR_NAME = "T_OILMASS_ERR";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT), // 1
			new Column("F_VEHICLE_ID", Types.BIGINT), // 2
			new Column("F_LATITUDE", Types.INTEGER), // 3
			new Column("F_LONGITUDE", Types.INTEGER), // 4
			new Column("F_HIGH", Types.SMALLINT), // 5
			new Column("F_SPEED", Types.SMALLINT), // 6
			new Column("F_DIRECTION", Types.SMALLINT), // 7
			new Column("F_GPS_TIME", Types.TIMESTAMP), // 8
			new Column("F_FLAG", Types.SMALLINT), // 9
			new Column("F_LIQUIDLEVEL", Types.DOUBLE), // 10
			new Column("F_FUELCHARGE", Types.DOUBLE), // 11
			new Column("F_OILMASS", Types.DOUBLE), // 12
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32), // 13
			new Column("F_DECREASE", Types.DOUBLE), // 14
			new Column("F_LIQUIDLEVEL_INCREASE", Types.DOUBLE), // 15
			new Column("F_LIQUIDLEVEL_DECREASE", Types.DOUBLE), // 16
			new Column("F_IS_AGG", Types.SMALLINT), // 17
			new Column("F_PROTOCOL_VER", Types.SMALLINT) // 18
	};

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> oilmassList)
			throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = null;
		Map<String, List<Map<Integer, Object>>> tableListMap = new HashMap<String, List<Map<Integer, Object>>>();
		String INSERT_PREPARED_SQL = null;
		for (int i = 0, size = oilmassList.size(); i < size; i++) {
			try {
				OilMass oilmass = OilMass.parseFrom(oilmassList.get(i));
				if (DateUtil.subtractOneDay() <= oilmass.getGpsTime() && DateUtil.addOneDay() >= oilmass.getGpsTime()) {
					String gpsTime = DateUtil.getStrTime(oilmass.getGpsTime(), "yyyyMMdd");
					String TABLE_NAME_DATE = TABLE_NAME + gpsTime;
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME_DATE, COLUMNS);
				} else {
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERR_NAME, COLUMNS);
				}
				if (!tableListMap.containsKey(INSERT_PREPARED_SQL)) {
					s = new ArrayList<Map<Integer, Object>>();
					s.add(buildSqlObjectListFromOilMass(oilmass));
					tableListMap.put(INSERT_PREPARED_SQL, s);
				} else {
					tableListMap.get(INSERT_PREPARED_SQL).add(buildSqlObjectListFromOilMass(oilmass));
				}
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse OilMass error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		for (String key : tableListMap.keySet()) {
			preparedSqlAndValuesList.add(new Insert(key, COLUMNS, tableListMap.get(key)));
		}
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromOilMass(OilMass oilmass) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, oilmass.getID());
		m.put(2, oilmass.getVehicleID());
		m.put(3, oilmass.getLatitude());
		m.put(4, oilmass.getLongitude());
		m.put(5, oilmass.getHigh());
		m.put(6, oilmass.getSpeed());
		m.put(7, oilmass.getDirection());
		m.put(8, new java.sql.Date(oilmass.getGpsTime()));
		m.put(9, oilmass.getFlag());
		m.put(10, oilmass.getLiquidLevel());
		m.put(11, oilmass.getFuelCharge());
		m.put(12, oilmass.getOilMassValue());
		m.put(13, oilmass.getEnterpriseCode());
		m.put(14, oilmass.getDecrease());
		m.put(15, oilmass.getLiquidLevelIncrease());
		m.put(16, oilmass.getLiquidLevelDecrease());
		m.put(17, oilmass.getIsAgg() ? 1 : 0);
		m.put(18, oilmass.getProtocolVer());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> oilmassList)
			throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERR_NAME, COLUMNS);
		for (int i = 0, size = oilmassList.size(); i < size; i++) {
			try {
				OilMass oilmass = OilMass.parseFrom(oilmassList.get(i));
				s.add(buildSqlObjectListFromOilMass(oilmass));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse OilMass error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}
}