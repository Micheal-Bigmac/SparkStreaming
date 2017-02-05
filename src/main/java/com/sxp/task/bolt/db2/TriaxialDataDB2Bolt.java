package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.TriaxiaDataInfo.TriaxialData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TriaxialDataDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(TriaxialDataDB2Bolt.class);
	static String TABLE_NAME = "T_TRIAXIALSENSORREPORT";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_LATITUDE", Types.INTEGER),// 3
			new Column("F_LONGITUDE", Types.INTEGER),// 4
			new Column("F_HIGH", Types.SMALLINT),// 5
			new Column("F_SPEED", Types.SMALLINT),// 6
			new Column("F_DIRECTION", Types.SMALLINT),// 7
			new Column("F_GPS_TIME", Types.TIMESTAMP),// 8
			new Column("F_RECV_TIME", Types.TIMESTAMP),// 9
			new Column("F_XANGLE", Types.SMALLINT),// 10
			new Column("F_YANGLE", Types.SMALLINT),// 11
			new Column("F_ZANGLE", Types.SMALLINT),// 12
			new Column("F_XACCELERATION", Types.INTEGER),// 13
			new Column("F_YACCELERATION", Types.INTEGER),// 14
			new Column("F_ZACCELERATION", Types.INTEGER),// 15
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32),// 16
			new Column("F_TEMPERATURE", Types.SMALLINT) // 17
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> triaxialdataList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = triaxialdataList.size(); i < size; i++) {
			try {
				TriaxialData triaxialdata = TriaxialData.parseFrom(triaxialdataList.get(i));
				s.add(buildSqlObjectMapFromTriaxialData(triaxialdata));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse TriaxialData error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectMapFromTriaxialData(TriaxialData triaxialdata) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, triaxialdata.getID());
		m.put(2, triaxialdata.getVehicleID());
		m.put(3, triaxialdata.getLatitude());
		m.put(4, triaxialdata.getLongitude());
		m.put(5, triaxialdata.getHigh());
		m.put(6, triaxialdata.getSpeed());
		m.put(7, triaxialdata.getDirection());
		m.put(8, new java.sql.Date(triaxialdata.getGpsTime()));
		m.put(9, new java.sql.Date(triaxialdata.getRecvTime()));
		m.put(10, triaxialdata.getXAngle());
		m.put(11, triaxialdata.getYAngle());
		m.put(12, triaxialdata.getZAngle());
		m.put(13, triaxialdata.getXAcceleration());
		m.put(14, triaxialdata.getYAcceleration());
		m.put(15, triaxialdata.getZAcceleration());
		m.put(16, triaxialdata.getEnterpriseCode());
		m.put(17, triaxialdata.getTemperature());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}