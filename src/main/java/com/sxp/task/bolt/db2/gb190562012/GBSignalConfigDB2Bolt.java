package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBSignalConfig;
import com.sxp.task.protobuf.generated.Gb2012.GBSignalConfigD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBSignalConfigDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBSignalConfigDB2Bolt.class);
	static String TABLE_NAME = "T_DR_SIGNAL_CONFIGURATION_INFORMATION";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_PLATE_CODE", Types.VARCHAR, 32),// 3
			new Column("F_ENTERPRISE_CODE", Types.BIGINT),// 4
			new Column("F_TERMINAL_ID", Types.BIGINT),// 5
			new Column("F_TERMINAL_CODE", Types.VARCHAR, 32),// 6
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 7
			new Column("F_ORDER_ID", Types.BIGINT),// 8
			new Column("F_DR_TIME", Types.TIMESTAMP),// 9
			new Column("F_CONFIGURATION_COUNT", Types.SMALLINT) // 10
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);
	static String SUB_TABLE_NAME = "T_DR_SIGNAL_CONFIGURATION_INFORMATION_EXTENSION";
	public static Column[] COLUMNS_SUB = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_CODE", Types.SMALLINT),// 2
			new Column("F_SIGNALNAME0", Types.VARCHAR, 10),// 3
			new Column("F_SIGNALNAME1", Types.VARCHAR, 10),// 4
			new Column("F_SIGNALNAME2", Types.VARCHAR, 10),// 5
			new Column("F_SIGNALNAME3", Types.VARCHAR, 10),// 6
			new Column("F_SIGNALNAME4", Types.VARCHAR, 10),// 7
			new Column("F_SIGNALNAME5", Types.VARCHAR, 10),// 8
			new Column("F_SIGNALNAME6", Types.VARCHAR, 10),// 9
			new Column("F_SIGNALNAME7", Types.VARCHAR, 10) // 10
	};
	public static final String SUB_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(SUB_TABLE_NAME, COLUMNS_SUB);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gbsignalconfigList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gbsignalconfigList.size(); i < size; i++) {
			try {
				GBSignalConfig gbsignalconfig = GBSignalConfig.parseFrom(gbsignalconfigList.get(i));
				s.add(buildSqlObjectListFromGBSignalConfig(gbsignalconfig));
				sD.addAll(buildSqlObjectListFromGBSignalConfigD(gbsignalconfig.getItemsList()));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBSignalConfig error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		preparedSqlAndValuesList.add(new Insert(SUB_INSERT_PREPARED_SQL, COLUMNS_SUB, sD));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromGBSignalConfig(GBSignalConfig gbsignalconfig) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gbsignalconfig.getID());
		m.put(2, gbsignalconfig.getVehicleID());
		m.put(3, gbsignalconfig.getVehicleName());
		m.put(4, gbsignalconfig.getEnterpriseCode());
		m.put(5, gbsignalconfig.getTerminalID());
		m.put(6, gbsignalconfig.getTerminalCode());
		m.put(7, new java.sql.Date(gbsignalconfig.getRecordTime()));
		m.put(8, gbsignalconfig.getOrderID());
		m.put(9, new java.sql.Date(gbsignalconfig.getRealTime()));
		m.put(10, gbsignalconfig.getItemCount());
		return m;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromGBSignalConfigD(List<GBSignalConfigD> itemsList) {
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (GBSignalConfigD gbsignalconfigd : itemsList) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, gbsignalconfigd.getID());
			m.put(2, gbsignalconfigd.getStateSignal());
			m.put(3, gbsignalconfigd.getSignalName0());
			m.put(4, gbsignalconfigd.getSignalName1());
			m.put(5, gbsignalconfigd.getSignalName2());
			m.put(6, gbsignalconfigd.getSignalName3());
			m.put(7, gbsignalconfigd.getSignalName4());
			m.put(8, gbsignalconfigd.getSignalName5());
			m.put(9, gbsignalconfigd.getSignalName6());
			m.put(10, gbsignalconfigd.getSignalName7());
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