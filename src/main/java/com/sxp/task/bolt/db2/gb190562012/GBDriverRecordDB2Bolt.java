package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBDriverRecord;
import com.sxp.task.protobuf.generated.Gb2012.GBDriverRecordD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBDriverRecordDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBDriverRecordDB2Bolt.class);
	static String TABLE_NAME = "T_GBDRIVERRECORD";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_ORDER_ID", Types.BIGINT),// 3
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 4
			new Column("F_COUNT", Types.SMALLINT),// 5
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 6
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);
	static String SUB_TABLE_NAME = "T_GBDRIVERRECORDDETAIL";
	public static Column[] COLUMNS_SUB = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 2
			new Column("F_DRIVINGLICENSE", Types.VARCHAR, 20),// 3
			new Column("F_TYPE", Types.SMALLINT),// 4
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32),// 5
			new Column("F_GBRD_ID", Types.BIGINT),// 6
			new Column("F_ISBEOVERDUE", Types.SMALLINT),// 7
			new Column("F_VEHICLE_ID", Types.BIGINT) // 8
	};
	public static final String SUB_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(SUB_TABLE_NAME, COLUMNS_SUB);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gbdriverrecordList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gbdriverrecordList.size(); i < size; i++) {
			try {
				GBDriverRecord gbdriverrecord = GBDriverRecord.parseFrom(gbdriverrecordList.get(i));
				s.add(buildSqlObjectListFromGBDriverRecord(gbdriverrecord));
				sD.addAll(buildSqlObjectListFromGBDriverRecordD(gbdriverrecord));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBDriverRecord error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		preparedSqlAndValuesList.add(new Insert(SUB_INSERT_PREPARED_SQL, COLUMNS_SUB, sD));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromGBDriverRecord(GBDriverRecord gbdriverrecord) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gbdriverrecord.getID());
		m.put(2, gbdriverrecord.getVehicleID());
		m.put(3, gbdriverrecord.getOrderID());
		m.put(4, new java.sql.Date(gbdriverrecord.getRecordTime()));
		m.put(5, gbdriverrecord.getCount());
		m.put(6, gbdriverrecord.getEnterpriseCode());
		return m;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromGBDriverRecordD(GBDriverRecord gbdriverrecord) {
		List<GBDriverRecordD> itemsList = gbdriverrecord.getItemsList();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (GBDriverRecordD gbdriverrecordd : itemsList) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, gbdriverrecordd.getID());
			m.put(2, new java.sql.Date(gbdriverrecordd.getStartTime()));
			m.put(3, gbdriverrecordd.getDrivingLicense());
			m.put(4, gbdriverrecordd.getEventType());
			m.put(5, gbdriverrecord.getEnterpriseCode());
			m.put(6, gbdriverrecord.getID());
			m.put(7, gbdriverrecordd.getIsBeoverdue() ? 1 : 0);
			m.put(8, gbdriverrecord.getVehicleID());
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