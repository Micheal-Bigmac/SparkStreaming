package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBParameterModify;
import com.sxp.task.protobuf.generated.Gb2012.GBParameterModifyD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBParameterModifyDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBParameterModifyDB2Bolt.class);
	static String TABLE_NAME = "T_GBPARAMETERMODIFY";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_ORDER_ID", Types.BIGINT),// 3
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 4
			new Column("F_COUNT", Types.SMALLINT),// 5
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 6
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);
	static String SUB_TABLE_NAME = "T_GBPARAMETERMODIFYDETAIL";
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
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gbparametermodifyList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gbparametermodifyList.size(); i < size; i++) {
			try {
				GBParameterModify gbparametermodify = GBParameterModify.parseFrom(gbparametermodifyList.get(i));
				s.add(buildSqlObjectListFromGBParameterModify(gbparametermodify));
				sD.addAll(buildSqlObjectListFromGBParameterModifyD(gbparametermodify));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBParameterModify error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		preparedSqlAndValuesList.add(new Insert(SUB_INSERT_PREPARED_SQL, COLUMNS_SUB, sD));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromGBParameterModify(GBParameterModify gbparametermodify) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gbparametermodify.getID());
		m.put(2, gbparametermodify.getVehicleID());
		m.put(3, gbparametermodify.getOrderID());
		m.put(4, new java.sql.Date(gbparametermodify.getRecordTime()));
		m.put(5, gbparametermodify.getCount());
		m.put(6, gbparametermodify.getEnterpriseCode());
		return m;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromGBParameterModifyD(GBParameterModify gbparametermodify) {
		List<GBParameterModifyD> itemsList = gbparametermodify.getItemsList();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (GBParameterModifyD gbparametermodifyd : itemsList) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, gbparametermodifyd.getID());
			m.put(2, new java.sql.Date(gbparametermodifyd.getStartTime()));
			m.put(3, gbparametermodifyd.getEventType());
			m.put(4, gbparametermodify.getEnterpriseCode());
			m.put(5, gbparametermodify.getID());
			m.put(6, gbparametermodifyd.getIsBeoverdue() ? 1 : 0);
			m.put(7, gbparametermodify.getVehicleID());
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