package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBUniqueNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBUniqueNumberDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBUniqueNumberDB2Bolt.class);
	static String TABLE_NAME = "T_DR_UNIQUE_ID_INFORMATION";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_PLATE_CODE", Types.VARCHAR, 32),// 3
			new Column("F_ENTERPRISE_CODE", Types.BIGINT),// 4
			new Column("F_TERMINAL_ID", Types.BIGINT),// 5
			new Column("F_TERMINAL_CODE", Types.VARCHAR, 32),// 6
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 7
			new Column("F_ORDER_ID", Types.BIGINT),// 8
			new Column("F_CCC_AUTHENTICATION_CODE", Types.VARCHAR, 8),// 9
			new Column("F_MODIFY_DOCUMENT_CODE", Types.VARCHAR, 16),// 10
			new Column("F_DR_PRODUCTION_TIME", Types.TIMESTAMP),// 11
			new Column("F_PRODUCTION_SERIAL_CODE", Types.INTEGER) // 12
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gbuniquenumberList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gbuniquenumberList.size(); i < size; i++) {
			try {
				GBUniqueNumber gbuniquenumber = GBUniqueNumber.parseFrom(gbuniquenumberList.get(i));
				s.add(buildSqlObjectMapFromGBUniqueNumber(gbuniquenumber));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBUniqueNumber error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectMapFromGBUniqueNumber(GBUniqueNumber gbuniquenumber) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gbuniquenumber.getID());
		m.put(2, gbuniquenumber.getVehicleID());
		m.put(3, gbuniquenumber.getVehicleName());
		m.put(4, gbuniquenumber.getEnterpriseCode());
		m.put(5, gbuniquenumber.getTerminalID());
		m.put(6, gbuniquenumber.getTerminalCode());
		m.put(7, new java.sql.Date(gbuniquenumber.getRecordTime()));
		m.put(8, gbuniquenumber.getOrderID());
		m.put(9, gbuniquenumber.getCCCCode());
		m.put(10, gbuniquenumber.getAuthenticationType());
		m.put(11, new java.sql.Date(gbuniquenumber.getProductDate()));
		m.put(12, gbuniquenumber.getSerialNumber());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}