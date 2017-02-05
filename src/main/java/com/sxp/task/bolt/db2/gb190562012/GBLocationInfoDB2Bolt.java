package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBLocationInfo;
import com.sxp.task.protobuf.generated.Gb2012.GBLocationInfoD;
import com.sxp.task.protobuf.generated.Gb2012.GBLocationInfoDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBLocationInfoDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBLocationInfoDB2Bolt.class);
	static String TABLE_NAME = "T_GBLOCATIONINFO";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_ORDER_ID", Types.BIGINT),// 3
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 4
			new Column("F_COUNT", Types.SMALLINT),// 5
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 6
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);
	static String SUB_TABLE_NAME = "T_GBLOCATIONINFODETAILS";
	public static Column[] COLUMNS_SUB = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_GBID_ID", Types.BIGINT),// 2
			new Column("F_STARTTIME", Types.TIMESTAMP),// 3
			new Column("F_NUMBER", Types.SMALLINT),// 4
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32),// 5
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 6
			new Column("F_VEHICLE_ID", Types.BIGINT),// 7
			new Column("F_ISBEOVERDUE", Types.SMALLINT) // 8
	};
	public static final String SUB_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(SUB_TABLE_NAME, COLUMNS_SUB);
	static String SUB_SUB_TABLE_NAME = "T_GBLOCATIONINFODETAIL";
	public static Column[] COLUMNS_SUB_SUB = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_GBIDS_ID", Types.BIGINT),// 2
			new Column("F_NUMBER", Types.BIGINT),// 3
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 4
			new Column("F_ONEMINUTEAVGSPEED", Types.SMALLINT),// 5
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32),// 6
			new Column("F_LONGITUDE", Types.INTEGER),// 7
			new Column("F_LATITUDE", Types.INTEGER),// 8
			new Column("F_ELEVATION", Types.SMALLINT),// 9
			new Column("F_VEHICLE_ID", Types.BIGINT) // 10
	};
	public static final String SUB_SUB_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(SUB_SUB_TABLE_NAME, COLUMNS_SUB_SUB);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gblocationinfoList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> sDD = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gblocationinfoList.size(); i < size; i++) {
			try {
				GBLocationInfo gblocationinfo = GBLocationInfo.parseFrom(gblocationinfoList.get(i));
				s.add(buildSqlObjectListFromGBLocationInfo(gblocationinfo));
				sD.addAll(buildSqlObjectListFromGBLocationInfoD(gblocationinfo));
				sDD.addAll(buildSqlObjectListFromGBLocationInfoDD(gblocationinfo));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBLocationInfo error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		preparedSqlAndValuesList.add(new Insert(SUB_INSERT_PREPARED_SQL, COLUMNS_SUB, sD));
		preparedSqlAndValuesList.add(new Insert(SUB_SUB_INSERT_PREPARED_SQL, COLUMNS_SUB_SUB, sDD));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromGBLocationInfo(GBLocationInfo gblocationinfo) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gblocationinfo.getID());
		m.put(2, gblocationinfo.getVehicleID());
		m.put(3, gblocationinfo.getOrderID());
		m.put(4, new java.sql.Date(gblocationinfo.getRecordTime()));
		m.put(5, gblocationinfo.getCount());
		m.put(6, gblocationinfo.getEnterpriseCode());
		return m;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromGBLocationInfoD(GBLocationInfo gblocationinfo) {
		List<GBLocationInfoD> itemsList = gblocationinfo.getItemsList();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (GBLocationInfoD gblocationinfod : itemsList) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, gblocationinfod.getID());
			m.put(2, gblocationinfo.getID());
			m.put(3, new java.sql.Date(gblocationinfod.getStartTime()));
			m.put(4, gblocationinfod.getNumber());
			m.put(5, gblocationinfo.getEnterpriseCode());
			m.put(6, gblocationinfo.getRecordTime());
			m.put(7, gblocationinfo.getVehicleID());
			m.put(8, gblocationinfod.getIsBeoverdue() ? 1 : 0);
			sD.add(m);
		}
		return sD;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromGBLocationInfoDD(GBLocationInfo gblocationinfo) {
		List<Map<Integer, Object>> sDD = new ArrayList<Map<Integer, Object>>();
		List<GBLocationInfoD> itemsList = gblocationinfo.getItemsList();
		for (GBLocationInfoD gbLocationInfoD : itemsList) {
			List<GBLocationInfoDD> itemsList2 = gbLocationInfoD.getItemsList();
			for (GBLocationInfoDD gblocationinfodd : itemsList2) {
				Map<Integer, Object> m = new HashMap<Integer, Object>();
				m.put(1, gblocationinfodd.getID());
				m.put(2, gbLocationInfoD.getID());
				m.put(3, gblocationinfodd.getNumber());
				m.put(4, new java.sql.Date(gblocationinfo.getRecordTime()));
				m.put(5, gblocationinfodd.getOneMinuteAvgSpeed());
				m.put(6, gblocationinfo.getEnterpriseCode());
				m.put(7, gblocationinfodd.getLongitude());
				m.put(8, gblocationinfodd.getLatitude());
				m.put(9, gblocationinfodd.getElevation());
				m.put(10, gblocationinfo.getVehicleID());
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