package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.WayBillInfo.WayBill;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WayBillDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(WayBillDB2Bolt.class);
	static String TABLE_NAME = "T_CURRENT_EWAYBILL";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_EWAYBILL_LENGTH", Types.INTEGER),// 2
			new Column("F_EWAYBILL_CONTENT", Types.VARCHAR),// 3
			new Column("F_CREATETIME", Types.TIMESTAMP),// 4
			new Column("F_TERMINAL_ID", Types.BIGINT),// 5
			new Column("F_VEHICLE_ID", Types.BIGINT),// 6
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32),// 7
			new Column("F_ISSENDTOMON", Types.SMALLINT) // 8
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> waybillList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = waybillList.size(); i < size; i++) {
			try {
				WayBill waybill = WayBill.parseFrom(waybillList.get(i));
				s.add(buildSqlObjectMapFromWayBill(waybill));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse WayBill error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectMapFromWayBill(WayBill waybill) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, waybill.getID());
		m.put(2, waybill.getContent().length());
		m.put(3, waybill.getContent());
		m.put(4, new java.sql.Date(waybill.getCreateTime()));
		m.put(5, waybill.getDeviceID());
		m.put(6, waybill.getVehicleID());
		m.put(7, waybill.getEnterpriseCode());
		m.put(8, waybill.getReported() ? 1 : 0);
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}