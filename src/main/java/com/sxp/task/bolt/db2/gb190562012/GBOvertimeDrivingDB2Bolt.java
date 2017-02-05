package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBOvertimeDriving;
import com.sxp.task.protobuf.generated.Gb2012.GBOvertimeDrivingD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBOvertimeDrivingDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBOvertimeDrivingDB2Bolt.class);
	static String TABLE_NAME = "T_GBOVERTIMEDRIVING";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_ORDER_ID", Types.BIGINT),// 3
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 4
			new Column("F_COUNT", Types.SMALLINT),// 5
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 6
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);
	static String SUB_TABLE_NAME = "T_GBOVERTIMEDRIVINGDETAIL";
	public static Column[] COLUMNS_SUB = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_GBOD_ID", Types.BIGINT),// 2
			new Column("F_DRIVINGLICENSE", Types.VARCHAR, 18),// 3
			new Column("F_STARTTIME", Types.TIMESTAMP),// 4
			new Column("F_ENDTIME", Types.TIMESTAMP),// 5
			new Column("F_LONGITUDE_START", Types.INTEGER),// 6
			new Column("F_LATITUDE_START", Types.INTEGER),// 7
			new Column("F_ELEVATION_START", Types.SMALLINT),// 8
			new Column("F_LONGITUDE_END", Types.INTEGER),// 9
			new Column("F_LATITUDE_END", Types.INTEGER),// 10
			new Column("F_ELEVATION_END", Types.SMALLINT),// 11
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32),// 12
			new Column("F_VEHICLE_ID", Types.BIGINT),// 13
			new Column("F_ISBEOVERDUE", Types.SMALLINT) // 14
	};
	public static final String SUB_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(SUB_TABLE_NAME, COLUMNS_SUB);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gbovertimedrivingList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gbovertimedrivingList.size(); i < size; i++) {
			try {
				GBOvertimeDriving gbovertimedriving = GBOvertimeDriving.parseFrom(gbovertimedrivingList.get(i));
				s.add(buildSqlObjectListFromGBOvertimeDriving(gbovertimedriving));
				sD.addAll(buildSqlObjectListFromGBOvertimeDrivingD(gbovertimedriving));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBOvertimeDriving error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		preparedSqlAndValuesList.add(new Insert(SUB_INSERT_PREPARED_SQL, COLUMNS_SUB, sD));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromGBOvertimeDriving(GBOvertimeDriving gbovertimedriving) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gbovertimedriving.getID());
		m.put(2, gbovertimedriving.getVehicleID());
		m.put(3, gbovertimedriving.getOrderID());
		m.put(4, new java.sql.Date(gbovertimedriving.getRecordTime()));
		m.put(5, gbovertimedriving.getCount());
		m.put(6, gbovertimedriving.getEnterpriseCode());
		return m;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromGBOvertimeDrivingD(GBOvertimeDriving gbovertimedriving) {
		List<GBOvertimeDrivingD> itemsList = gbovertimedriving.getItemsList();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (GBOvertimeDrivingD gbovertimedrivingd : itemsList) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, gbovertimedrivingd.getID());
			m.put(2, gbovertimedriving.getID());
			m.put(3, gbovertimedrivingd.getDrivingLicense());
			m.put(4, new java.sql.Date(gbovertimedrivingd.getStartTime()));
			m.put(5, new java.sql.Date(gbovertimedrivingd.getEndTime()));
			m.put(6, gbovertimedrivingd.getLongitudeS());
			m.put(7, gbovertimedrivingd.getLatitudeS());
			m.put(8, gbovertimedrivingd.getElevationS());
			m.put(9, gbovertimedrivingd.getLongitudeE());
			m.put(10, gbovertimedrivingd.getLatitudeE());
			m.put(11, gbovertimedrivingd.getElevationE());
			m.put(12, gbovertimedriving.getEnterpriseCode());
			m.put(13, gbovertimedriving.getVehicleID());
			m.put(14, gbovertimedrivingd.getIsBeoverdue() ? 1 : 0);
			sD.add(m);
		}
		return sD;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}