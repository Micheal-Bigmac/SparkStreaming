package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBSpeedState;
import com.sxp.task.protobuf.generated.Gb2012.GBSpeedStateD;
import com.sxp.task.protobuf.generated.Gb2012.GBSpeedStateDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBSpeedStateDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBSpeedStateDB2Bolt.class);
	static String TABLE_NAME = "T_GBSPEEDSTATE";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_ORDER_ID", Types.BIGINT),// 3
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 4
			new Column("F_COUNT", Types.SMALLINT),// 5
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 6
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);
	static String SUB_TABLE_NAME = "T_GBSPEEDSTATEDETAILS";
	public static Column[] COLUMNS_SUB = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_SPSTATE", Types.SMALLINT),// 2
			new Column("F_STARTTIME", Types.TIMESTAMP),// 3
			new Column("F_ENDTIME", Types.TIMESTAMP),// 4
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32),// 5
			new Column("F_GBRD_ID", Types.BIGINT),// 6
			new Column("F_ISBEOVERDUE", Types.SMALLINT),// 7
			new Column("F_VEHICLE_ID", Types.BIGINT) // 8
	};
	public static final String SUB_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(SUB_TABLE_NAME, COLUMNS_SUB);
	static String SUB_SUB_TABLE_NAME = "T_GBSPEEDSTATEDETAIL";
	public static Column[] COLUMNS_SUB_SUB = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_RECORDSPEED", Types.SMALLINT),// 2
			new Column("F_CONTRASSPEED", Types.SMALLINT),// 3
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32),// 4
			new Column("F_GBRD_ID", Types.BIGINT),// 5
			new Column("F_NUMBER", Types.SMALLINT) // 6
	};
	public static final String SUB_SUB_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(SUB_SUB_TABLE_NAME, COLUMNS_SUB_SUB);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gbspeedstateList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> sDD = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gbspeedstateList.size(); i < size; i++) {
			try {
				GBSpeedState gbspeedstate = GBSpeedState.parseFrom(gbspeedstateList.get(i));
				s.add(buildSqlObjectListFromGBSpeedState(gbspeedstate));
				sD.addAll(buildSqlObjectListFromGBSpeedStateD(gbspeedstate));
				sDD.addAll(buildSqlObjectListFromGBSpeedStateDD(gbspeedstate));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBSpeedState error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		preparedSqlAndValuesList.add(new Insert(SUB_INSERT_PREPARED_SQL, COLUMNS_SUB, sD));
		preparedSqlAndValuesList.add(new Insert(SUB_SUB_INSERT_PREPARED_SQL, COLUMNS_SUB_SUB, sDD));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromGBSpeedState(GBSpeedState gbspeedstate) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gbspeedstate.getID());
		m.put(2, gbspeedstate.getVehicleID());
		m.put(3, gbspeedstate.getOrderID());
		m.put(4, new java.sql.Date(gbspeedstate.getRecordTime()));
		m.put(5, gbspeedstate.getCount());
		m.put(6, gbspeedstate.getEnterpriseCode());
		return m;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromGBSpeedStateD(GBSpeedState gbspeedstate) {
		List<GBSpeedStateD> itemsList = gbspeedstate.getItemsList();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (GBSpeedStateD gbspeedstated : itemsList) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, gbspeedstated.getID());
			m.put(2, gbspeedstated.getSpeedStatue());
			m.put(3, new java.sql.Date(gbspeedstated.getStartTime()));
			m.put(4, new java.sql.Date(gbspeedstated.getEndTime()));
			m.put(5, gbspeedstate.getEnterpriseCode());
			m.put(6, gbspeedstate.getID());
			m.put(7, gbspeedstated.getIsBeoverdue() ? 1 : 0);
			m.put(8, gbspeedstate.getVehicleID());
			sD.add(m);
		}
		return sD;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromGBSpeedStateDD(GBSpeedState gbspeedstate) {
		List<Map<Integer, Object>> sDD = new ArrayList<Map<Integer, Object>>();
		List<GBSpeedStateD> itemsList = gbspeedstate.getItemsList();
		for (GBSpeedStateD gbspeedstated : itemsList) {
			List<GBSpeedStateDD> itemsList2 = gbspeedstated.getItemsList();
			for (GBSpeedStateDD gbspeedstatedd : itemsList2) {
				Map<Integer, Object> m = new HashMap<Integer, Object>();
				m.put(1, gbspeedstatedd.getID());
				m.put(2, gbspeedstated.getSpeedStatue());
				m.put(3, new java.sql.Date(gbspeedstated.getStartTime()));
				m.put(4, new java.sql.Date(gbspeedstated.getEndTime()));
				m.put(5, gbspeedstate.getEnterpriseCode());
				m.put(6, gbspeedstate.getID());
				m.put(7, gbspeedstated.getIsBeoverdue() ? 1 : 0);
				m.put(8, gbspeedstate.getVehicleID());
				sDD.add(m);
			}
		}
		return sDD;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}

}