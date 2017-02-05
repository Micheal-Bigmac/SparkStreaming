package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBExternalPower;
import com.sxp.task.protobuf.generated.Gb2012.GBExternalPowerD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBExternalPowerDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBExternalPowerDB2Bolt.class);
	static String TABLE_NAME = "T_GBEXTERNALPOWER";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_ORDER_ID", Types.BIGINT),// 3
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 4
			new Column("F_COUNT", Types.SMALLINT),// 5
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 6
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);
	static String SUB_TABLE_NAME = "T_GBEXTERNALPOWERDETAIL";
	public static Column[] COLUMNS_SUB = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 2
			new Column("F_TYPE", Types.SMALLINT),// 3
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32),// 4
			new Column("F_GBRD_ID", Types.BIGINT),// 5
			new Column("F_ISBEOVERDUE", Types.SMALLINT),// 6
			new Column("F_VEHICLE_ID", Types.BIGINT) // 7
	};
	public static final String SUB_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(SUB_TABLE_NAME, COLUMNS_SUB);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gbexternalpowerList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gbexternalpowerList.size(); i < size; i++) {
			try {
				GBExternalPower gbexternalpower = GBExternalPower.parseFrom(gbexternalpowerList.get(i));
				s.add(buildSqlObjectListFromGBExternalPower(gbexternalpower));
				sD.addAll(buildSqlObjectListFromGBExternalPowerD(gbexternalpower));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBExternalPower error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		preparedSqlAndValuesList.add(new Insert(SUB_INSERT_PREPARED_SQL, COLUMNS_SUB, sD));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromGBExternalPower(GBExternalPower gbexternalpower) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gbexternalpower.getID());
		m.put(2, gbexternalpower.getVehicleID());
		m.put(3, gbexternalpower.getOrderID());
		m.put(4, new java.sql.Date(gbexternalpower.getRecordTime()));
		m.put(5, gbexternalpower.getCount());
		m.put(6, gbexternalpower.getEnterpriseCode());
		return m;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromGBExternalPowerD(GBExternalPower gbexternalpower) {
		List<GBExternalPowerD> itemsList = gbexternalpower.getItemsList();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (GBExternalPowerD gbexternalpowerd : itemsList) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, gbexternalpowerd.getID());
			m.put(2, new java.sql.Date(gbexternalpowerd.getStartTime()));
			m.put(3, gbexternalpowerd.getEventType());
			m.put(4, gbexternalpower.getEnterpriseCode());
			m.put(5, gbexternalpower.getID());
			m.put(6, gbexternalpowerd.getIsBeoverdue() ? 1 : 0);
			m.put(7, gbexternalpower.getVehicleID());
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