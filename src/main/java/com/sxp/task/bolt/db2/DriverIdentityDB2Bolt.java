package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.DriverIdentityInfo.DriverIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DriverIdentityDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(DriverIdentityDB2Bolt.class);
	static String TABLE_NAME = "T_DRIVER_CHECK_LOG";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_DRIVERNAME", Types.VARCHAR, 20),// 3
			new Column("F_IDENTITY", Types.VARCHAR, 20),// 4
			new Column("F_DRIVERLICENSE", Types.VARCHAR, 40),// 5
			new Column("F_LICENSEORGA", Types.VARCHAR, 50),// 6
			new Column("F_CHECK_TIME", Types.TIMESTAMP),// 7
			new Column("F_SEND_UP", Types.SMALLINT),// 8
			// new Column("F_USER_ID", Types.BIGINT),// 9
			new Column("F_REV_TIME", Types.TIMESTAMP),// 9
			// new Column("F_TERMINAL_ID", Types.BIGINT),// 10
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32),// 10
			new Column("F_STATE", Types.SMALLINT),// 11
			new Column("F_READERRESULT", Types.SMALLINT),// 12
			new Column("F_DOCUMENTSAREVALID", Types.DATE) // 13
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> driveridentityList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = driveridentityList.size(); i < size; i++) {
			try {
				DriverIdentity driveridentity = DriverIdentity.parseFrom(driveridentityList.get(i));
				s.add(buildSqlObjectMapFromDriverIdentity(driveridentity));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse DriverIdentity error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectMapFromDriverIdentity(DriverIdentity driveridentity) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, driveridentity.getID());
		m.put(2, driveridentity.getVehicleID());
		m.put(3, driveridentity.getDriverName());
		m.put(4, driveridentity.hasDriverCardID() ? driveridentity.getDriverCardID() : null);
		m.put(5, driveridentity.getDriverLicense());
		m.put(6, driveridentity.getLicenseOrgName());
		m.put(7, driveridentity.hasPunchTime() ? new java.sql.Date(driveridentity.getPunchTime()) : null);
		m.put(8, driveridentity.getReported() ? 1 : 0);
		m.put(9, new java.sql.Date(driveridentity.getRevTime()));
		m.put(10, driveridentity.getEnterpriseCode());
		m.put(11, driveridentity.hasState() ? driveridentity.getState() : null);
		m.put(12, driveridentity.hasReadResult() ? driveridentity.getReadResult() : null);
		m.put(13, driveridentity.hasTerm() ? new java.sql.Date(driveridentity.getTerm()) : null);
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}