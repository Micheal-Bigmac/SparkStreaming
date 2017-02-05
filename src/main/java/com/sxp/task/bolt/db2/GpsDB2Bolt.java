package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.hbase.util.DateUtil;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.GpsInfo.Gps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GpsDB2Bolt extends AbstractDB2Bolt {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(GpsDB2Bolt.class);
	static String TABLE_NAME = "T_LOCATION_";
	static String TABLE_ERROR_NAME = "T_LOCATION_ERR";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT), // 1
			new Column("F_VEHICLE_ID", Types.BIGINT), // 2
			new Column("F_LATITUDE", Types.INTEGER), // 3
			new Column("F_LONGITUDE", Types.INTEGER), // 4
			new Column("F_HIGH", Types.INTEGER), // 5
			new Column("F_SPEED", Types.SMALLINT), // 6
			new Column("F_DIRECTION", Types.SMALLINT), // 7
			new Column("F_ALARM_COUNT", Types.SMALLINT), // 8
			new Column("F_DSPEED", Types.SMALLINT), // 9
			new Column("F_OIL_LEVEL", Types.SMALLINT), // 10
			new Column("F_MILEAGE", Types.INTEGER), // 11
			new Column("F_ALARM_DATA", Types.INTEGER), // 12
			new Column("F_GPS_TIME", Types.TIMESTAMP), // 13
			new Column("F_RECV_TIME", Types.TIMESTAMP), // 14
			new Column("F_ACC_STATUS", Types.SMALLINT), // 15
			new Column("F_IS_REAL_LOCATION", Types.SMALLINT), // 16
			new Column("F_TERMINAL_ID", Types.BIGINT), // 17
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32), // 18
			new Column("F_GPSENCRYPT", Types.SMALLINT), // 19
			new Column("F_GPSSTATE", Types.BIGINT), // 20
			new Column("F_ISPASSUP", Types.SMALLINT), // 21
			new Column("F_ALARM_DATA1", Types.BIGINT), // 22
			new Column("F_AREASN", Types.VARCHAR, 20) // 23
	};

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {

		Map<String, List<Map<Integer, Object>>> tableListMap = new HashMap<String, List<Map<Integer, Object>>>();
		List<Map<Integer, Object>> s = null;
		String INSERT_PREPARED_SQL = NULL;
		for (int i = 0, size = gpsList.size(); i < size; i++) {
			try {
				Gps gps = Gps.parseFrom(gpsList.get(i));
                //对接收的数据进行预处理，GPS时间不是两天之内的插入对应的ERR表，否则插入对应的天维度表
				if (DateUtil.subtractOneDay() <= gps.getGpsDate() && DateUtil.addOneDay() >= gps.getGpsDate()) {
					String gpsTime = DateUtil.getStrTime(gps.getGpsDate(), "yyyyMMdd");
					String TABLE_NAME_DATE = TABLE_NAME + gpsTime;
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME_DATE, COLUMNS);
				} else {
					INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERROR_NAME, COLUMNS);
				}
				if (!tableListMap.containsKey(INSERT_PREPARED_SQL)) {
					s = new ArrayList<Map<Integer, Object>>();
					s.add(buildSqlObjectListFromGps(gps));
					tableListMap.put(INSERT_PREPARED_SQL, s);
				} else {
					tableListMap.get(INSERT_PREPARED_SQL).add(buildSqlObjectListFromGps(gps));
				}

			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse Gps error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		for (String key : tableListMap.keySet()) {
			preparedSqlAndValuesList.add(new Insert(key, COLUMNS, tableListMap.get(key)));
		}
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromGps(Gps gps) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gps.getLocationID());
		m.put(2, gps.getVehicleID());
		m.put(3, gps.getLatitude());
		m.put(4, gps.getLongitude());
		int high = gps.getHigh();
		if (high > Short.MAX_VALUE / 2) {
			high = (short) high - Short.MAX_VALUE / 2;
//			LOG.warn("gps{LocationID=" + gps.getLocationID() + ",VehicleID=" + gps.getVehicleID() + ",GpsDate="
//					+ new java.sql.Date(gps.getGpsDate()).toLocaleString() + ",Hig=" + gps.getHigh()
//					+ "}'s, high is over than 32767");
		}
		m.put(5, high);
		m.put(6, gps.getSpeed());
		m.put(7, gps.getDirection());
		m.put(8, gps.getAlarmCount());
		m.put(9, gps.hasDrSpeed() ? gps.getDrSpeed() : null);
		m.put(10, gps.hasOilLevel() ? gps.getOilLevel() : null);
		m.put(11, gps.hasMileage() ? gps.getMileage() : null);
		m.put(12, gps.getAlarmData());
		m.put(13, new java.sql.Date(gps.getGpsDate()));
		m.put(14, new java.sql.Date(gps.getReciveDate()));
		m.put(15, gps.getACC() ? 1 : 0);
		m.put(16, gps.getIsRealLocation() ? 1 : 0);
		m.put(17, gps.getTerminalID());
		m.put(18, gps.getEnterpriseCode());
		m.put(19, gps.getGpsEncrypt() ? 1 : 0);
		m.put(20, gps.getGpsState());
		m.put(21, gps.getIsPassup() ? 1 : 0);
		m.put(22, gps.getAlarmData1());
		m.put(23, gps.getArea());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_ERROR_NAME, COLUMNS);
		for (int i = 0, size = gpsList.size(); i < size; i++) {
			try {
				Gps gps = Gps.parseFrom(gpsList.get(i));
				s.add(buildSqlObjectListFromGps(gps));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse Gps error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}
}