package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBTotalMileage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBTotalMileageDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBTotalMileageDB2Bolt.class);
	static String TABLE_NAME = "T_DR_TOTAL_MILEAGE_RECORD";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_PLATE_CODE", Types.VARCHAR, 32),// 3
			new Column("F_ENTERPRISE_CODE", Types.BIGINT),// 4
			new Column("F_TERMINAL_ID", Types.BIGINT),// 5
			new Column("F_TERMINAL_CODE", Types.VARCHAR, 32),// 6
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 7
			new Column("F_ORDER_ID", Types.BIGINT),// 8
			new Column("F_DR_TIME", Types.TIMESTAMP),// 9
			new Column("F_DR_START_TIME", Types.TIMESTAMP),// 10
			new Column("F_START_MILEAGE", Types.DOUBLE),// 11
			new Column("F_TOTAL_MILEAGE", Types.DOUBLE) // 12
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gbtotalmileageList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gbtotalmileageList.size(); i < size; i++) {
			try {
				GBTotalMileage gbtotalmileage = GBTotalMileage.parseFrom(gbtotalmileageList.get(i));
				s.add(buildSqlObjectMapFromGBTotalMileage(gbtotalmileage));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBTotalMileage error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectMapFromGBTotalMileage(GBTotalMileage gbtotalmileage) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gbtotalmileage.getID());
		m.put(2, gbtotalmileage.getVehicleID());
		m.put(3, gbtotalmileage.getVehicleName());
		m.put(4, gbtotalmileage.getEnterpriseCode());
		m.put(5, gbtotalmileage.getTerminalID());
		m.put(6, gbtotalmileage.getTerminalCode());
		m.put(7, new java.sql.Date(gbtotalmileage.getRecordTime()));
		m.put(8, gbtotalmileage.getOrderID());
		m.put(9, new java.sql.Date(gbtotalmileage.getRealTime()));
		m.put(10, new java.sql.Date(gbtotalmileage.getSetupTime()));
		m.put(11, gbtotalmileage.getInitialMileage());
		m.put(12, gbtotalmileage.getTotalMileage());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}