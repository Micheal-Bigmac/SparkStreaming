package com.sxp.task.bolt.db2.gb190562012;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.bolt.db2.AbstractDB2Bolt;
import com.sxp.task.protobuf.generated.Gb2012.GBTravelSpeed;
import com.sxp.task.protobuf.generated.Gb2012.GBTravelSpeedD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GBTravelSpeedDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(GBTravelSpeedDB2Bolt.class);
	static String TABLE_NAME = "T_GBTRAVELSPEED";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_ORDER_ID", Types.BIGINT),// 2
			new Column("F_VEHICLE_ID", Types.BIGINT),// 3
			new Column("F_TIME", Types.TIMESTAMP),// 4
			new Column("F_ENTERPRISE", Types.VARCHAR, 32) // 5
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);
	static String SUB_TABLE_NAME = "T_GBTRAVELSPEEDDETAILS";
	public static Column[] COLUMNS_SUB = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_TRAVEL_ID", Types.BIGINT),// 2
			new Column("F_VEHICLE_ID", Types.BIGINT),// 3
			new Column("F_TIME", Types.TIMESTAMP),// 4
			new Column("F_S_AVG", Types.SMALLINT),// 5
			new Column("F_S_COUNT", Types.SMALLINT),// 6
			new Column("F_ENTERPRISE", Types.VARCHAR, 32),// 7
			new Column("F_ISBEOVERDUE", Types.SMALLINT), // 8
			new Column("F_S_0", Types.SMALLINT),// 9
			new Column("F_S_1", Types.SMALLINT),// 10
			new Column("F_S_2", Types.SMALLINT),// 11
			new Column("F_S_3", Types.SMALLINT),// 12
			new Column("F_S_4", Types.SMALLINT),// 13
			new Column("F_S_5", Types.SMALLINT),// 14
			new Column("F_S_6", Types.SMALLINT),// 15
			new Column("F_S_7", Types.SMALLINT),// 16
			new Column("F_S_8", Types.SMALLINT),// 17
			new Column("F_S_9", Types.SMALLINT),// 18
			new Column("F_S_10", Types.SMALLINT),// 19
			new Column("F_S_11", Types.SMALLINT),// 20
			new Column("F_S_12", Types.SMALLINT),// 21
			new Column("F_S_13", Types.SMALLINT),// 22
			new Column("F_S_14", Types.SMALLINT),// 23
			new Column("F_S_15", Types.SMALLINT),// 24
			new Column("F_S_16", Types.SMALLINT),// 25
			new Column("F_S_17", Types.SMALLINT),// 26
			new Column("F_S_18", Types.SMALLINT),// 27
			new Column("F_S_19", Types.SMALLINT),// 28
			new Column("F_S_20", Types.SMALLINT),// 29
			new Column("F_S_21", Types.SMALLINT),// 30
			new Column("F_S_22", Types.SMALLINT),// 31
			new Column("F_S_23", Types.SMALLINT),// 32
			new Column("F_S_24", Types.SMALLINT),// 33
			new Column("F_S_25", Types.SMALLINT),// 34
			new Column("F_S_26", Types.SMALLINT),// 35
			new Column("F_S_27", Types.SMALLINT),// 36
			new Column("F_S_28", Types.SMALLINT),// 37
			new Column("F_S_29", Types.SMALLINT),// 38
			new Column("F_S_30", Types.SMALLINT),// 39
			new Column("F_S_31", Types.SMALLINT),// 40
			new Column("F_S_32", Types.SMALLINT),// 41
			new Column("F_S_33", Types.SMALLINT),// 42
			new Column("F_S_34", Types.SMALLINT),// 43
			new Column("F_S_35", Types.SMALLINT),// 44
			new Column("F_S_36", Types.SMALLINT),// 45
			new Column("F_S_37", Types.SMALLINT),// 46
			new Column("F_S_38", Types.SMALLINT),// 47
			new Column("F_S_39", Types.SMALLINT),// 48
			new Column("F_S_40", Types.SMALLINT),// 49
			new Column("F_S_41", Types.SMALLINT),// 50
			new Column("F_S_42", Types.SMALLINT),// 51
			new Column("F_S_43", Types.SMALLINT),// 52
			new Column("F_S_44", Types.SMALLINT),// 53
			new Column("F_S_45", Types.SMALLINT),// 54
			new Column("F_S_46", Types.SMALLINT),// 55
			new Column("F_S_47", Types.SMALLINT),// 56
			new Column("F_S_48", Types.SMALLINT),// 57
			new Column("F_S_49", Types.SMALLINT),// 58
			new Column("F_S_50", Types.SMALLINT),// 59
			new Column("F_S_51", Types.SMALLINT),// 60
			new Column("F_S_52", Types.SMALLINT),// 61
			new Column("F_S_53", Types.SMALLINT),// 62
			new Column("F_S_54", Types.SMALLINT),// 63
			new Column("F_S_55", Types.SMALLINT),// 64
			new Column("F_S_56", Types.SMALLINT),// 65
			new Column("F_S_57", Types.SMALLINT),// 66
			new Column("F_S_58", Types.SMALLINT),// 67
			new Column("F_S_59", Types.SMALLINT) // 68
	};
	public static final String SUB_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(SUB_TABLE_NAME, COLUMNS_SUB);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gbtravelspeedList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = gbtravelspeedList.size(); i < size; i++) {
			try {
				GBTravelSpeed gbtravelspeed = GBTravelSpeed.parseFrom(gbtravelspeedList.get(i));
				s.add(buildSqlObjectListFromGBTravelSpeed(gbtravelspeed));
				sD.addAll(buildSqlObjectListFromGBTravelSpeedD(gbtravelspeed));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse GBTravelSpeed error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		preparedSqlAndValuesList.add(new Insert(SUB_INSERT_PREPARED_SQL, COLUMNS_SUB, sD));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromGBTravelSpeed(GBTravelSpeed gbtravelspeed) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, gbtravelspeed.getID());
		m.put(2, gbtravelspeed.getOrderID());
		m.put(3, gbtravelspeed.getVehicleID());
		m.put(4, new java.sql.Date(gbtravelspeed.getRecordTime()));
		m.put(5, gbtravelspeed.getEnterpriseCode());
		return m;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromGBTravelSpeedD(GBTravelSpeed g) {
		List<GBTravelSpeedD> itemsList = g.getItemsList();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (GBTravelSpeedD gbtravelspeedd : itemsList) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, gbtravelspeedd.getID());
			m.put(2, g.getID());
			m.put(3, g.getVehicleID());
			m.put(4, new java.sql.Date(gbtravelspeedd.getTime()));
			m.put(5, gbtravelspeedd.getSpeedAvg());
			m.put(6, gbtravelspeedd.getSpeedCount());
			m.put(7, g.getEnterpriseCode());
			m.put(8, gbtravelspeedd.getIsBoverRoude() ? 1 : 0);
			int index = 9;
			List<Integer> speedArraryList = gbtravelspeedd.getItemsList();
			for (Integer integer : speedArraryList) {
				m.put(index++, integer);
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