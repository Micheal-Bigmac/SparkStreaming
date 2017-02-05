package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.UnreportedGpsInfo.UnreportedGps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnreportedGpsDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(UnreportedGpsDB2Bolt.class);
	static String TABLE_NAME = "T_MONSENDFAILGPS";
	public static Column[] COLUMNS = new Column[] { new Column("ROWID", Types.INTEGER),// 1
			new Column("TRACKID", Types.BIGINT),// 2
			new Column("PLATENAME", Types.VARCHAR, 20),// 3
			new Column("PLATECOLOR", Types.SMALLINT),// 4
			new Column("F_GROUP_ID", Types.BIGINT) // 5
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> unreportedgpsList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = unreportedgpsList.size(); i < size; i++) {
			try {
				UnreportedGps unreportedgps = UnreportedGps.parseFrom(unreportedgpsList.get(i));
				s.add(buildSqlObjectMapFromUnreportedGps(unreportedgps));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse UnreportedGps error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectMapFromUnreportedGps(UnreportedGps unreportedgps) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, unreportedgps.getID());
		m.put(2, unreportedgps.getTackID());
		m.put(3, unreportedgps.getPlateName());
		m.put(4, unreportedgps.getPlateColor());
		m.put(5, unreportedgps.getGroupID());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}