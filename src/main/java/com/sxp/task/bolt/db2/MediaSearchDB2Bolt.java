package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.MediaInfo.MediaSearch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MediaSearchDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(MediaSearchDB2Bolt.class);
	static String TABLE_NAME = "T_MEDIA_SELECTBACK";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_MSID", Types.BIGINT),// 2
			new Column("F_MEDIA_TYPE", Types.SMALLINT),// 3
			new Column("F_CHANNEL", Types.SMALLINT),// 4
			new Column("F_EVENT_TYPE", Types.SMALLINT),// 5
			new Column("F_GPS_DATE", Types.TIMESTAMP),// 6
			new Column("F_LOCATION_ID", Types.BIGINT),// 7
			new Column("F_EVENTID", Types.BIGINT), // 8
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 9
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> mediasearchList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = mediasearchList.size(); i < size; i++) {
			try {
				MediaSearch mediasearch = MediaSearch.parseFrom(mediasearchList.get(i));
				s.add(buildSqlObjectListFromMediaSearch(mediasearch));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse MediaSearch error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromMediaSearch(MediaSearch mediasearch) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, mediasearch.getID());
		m.put(2, mediasearch.getMSID());
		m.put(3, mediasearch.getMediaType());
		m.put(4, mediasearch.getChannel());
		m.put(5, mediasearch.getEventType());
		m.put(6, new java.sql.Date(mediasearch.getGpsDate()));
		m.put(7, mediasearch.getLocationID());
		m.put(8, mediasearch.getEventID());
		m.put(9, mediasearch.getEnterpriseCode());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}