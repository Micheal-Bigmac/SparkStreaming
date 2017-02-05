package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBDoubtfulAccident;
import com.sxp.task.protobuf.generated.Gb2012.GBDoubtfulAccidentD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBDoubtfulAccidentDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBDoubtfulAccidentDB2Bolt.class);
	static String TABLE_NAME = "T_GBDOUBTFULACCIDENT";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_ENDTIME", Types.TIMESTAMP),// 2
			new Column("F_DRIVINGLICENSE", Types.VARCHAR, 20),// 3
			new Column("F_LONGITUDE", Types.INTEGER),// 4
			new Column("F_LATITUDE", Types.INTEGER),// 5
			new Column("F_ELEVATION", Types.SMALLINT),// 6
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32),// 7
			new Column("F_BRAKESPEED", Types.SMALLINT),// 8
			new Column("F_BRAKETIME", Types.REAL),// 9
			new Column("F_ORDER_ID", Types.BIGINT),// 10
			new Column("F_VEHICLE_ID", Types.BIGINT),// 11
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 12
			new Column("F_ISBEOVERDUE", Types.SMALLINT) // 13
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);
	static String SUB_TABLE_NAME = "T_GBDOUBTFULACCIDENTDETAIL";
	public static Column[] COLUMNS_SUB = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_GBDAD_ID", Types.BIGINT),// 2
			new Column("F_TIME", Types.TIMESTAMP),// 3
			new Column("F_SPEED", Types.SMALLINT),// 4
			new Column("F_SIGNALVAL", Types.SMALLINT),// 5
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32),// 6
			new Column("F_STATESIGNAL0", Types.SMALLINT),// 7
			new Column("F_STATESIGNAL1", Types.SMALLINT),// 8
			new Column("F_STATESIGNAL2", Types.SMALLINT),// 9
			new Column("F_STATESIGNAL3", Types.SMALLINT),// 10
			new Column("F_STATESIGNAL4", Types.SMALLINT),// 11
			new Column("F_STATESIGNAL5", Types.SMALLINT),// 12
			new Column("F_STATESIGNAL6", Types.SMALLINT),// 13
			new Column("F_STATESIGNAL7", Types.SMALLINT) // 14
	};
	public static final String SUB_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(SUB_TABLE_NAME, COLUMNS_SUB);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gbdoubtfulaccidentList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gbdoubtfulaccidentList.size(); i < size; i++) {
			try {
				GBDoubtfulAccident gbdoubtfulaccident = GBDoubtfulAccident.parseFrom(gbdoubtfulaccidentList.get(i));
				s.add(buildSqlObjectListFromGBDoubtfulAccident(gbdoubtfulaccident));
				sD.addAll(buildSqlObjectListFromGBDoubtfulAccidentD(gbdoubtfulaccident));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBDoubtfulAccident error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		preparedSqlAndValuesList.add(new Insert(SUB_INSERT_PREPARED_SQL, COLUMNS_SUB, sD));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromGBDoubtfulAccident(GBDoubtfulAccident gbdoubtfulaccident) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gbdoubtfulaccident.getID());
		m.put(2, new java.sql.Date(gbdoubtfulaccident.getEndTime()));
		m.put(3, gbdoubtfulaccident.getDrivingLicense());
		m.put(4, gbdoubtfulaccident.getLongitude());
		m.put(5, gbdoubtfulaccident.getLatitude());
		m.put(6, gbdoubtfulaccident.getElevation());
		m.put(7, gbdoubtfulaccident.getEnterpriseCode());
		m.put(8, gbdoubtfulaccident.getBrakeSpeed());
		m.put(9, gbdoubtfulaccident.getBrakeTime());
		m.put(10, gbdoubtfulaccident.getOrderID());
		m.put(11, gbdoubtfulaccident.getVehicleID());
		m.put(12, gbdoubtfulaccident.getRecordTime());
		m.put(13, gbdoubtfulaccident.getIsBeoverdue() ? 1 : 0);
		return m;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromGBDoubtfulAccidentD(GBDoubtfulAccident gbdoubtfulaccident) {
		List<GBDoubtfulAccidentD> itemsList = gbdoubtfulaccident.getItemsList();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (GBDoubtfulAccidentD gbdoubtfulaccidentd : itemsList) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, gbdoubtfulaccidentd.getID());
			m.put(2, gbdoubtfulaccident.getID());
			m.put(3, new java.sql.Date(gbdoubtfulaccidentd.getTime()));
			m.put(4, gbdoubtfulaccidentd.getSpeed());
			m.put(5, gbdoubtfulaccidentd.getSignalVal());
			m.put(6, gbdoubtfulaccident.getEnterpriseCode());
			int index = 7;
			List<Boolean> signalArraryList = gbdoubtfulaccidentd.getSignalArraryList();
			for (Boolean boolean1 : signalArraryList) {
				m.put(index++, boolean1 ? 1 : 0);
			}
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