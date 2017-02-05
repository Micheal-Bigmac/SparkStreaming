package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.MediaInfo.MediaEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MediaEventDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(MediaEventDB2Bolt.class);
	static String TABLE_NAME = "T_MEDIA_EVENTS";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLEID", Types.BIGINT),// 2
			new Column("F_EVENTID", Types.BIGINT),// 3
			new Column("F_MEDIATYPE", Types.SMALLINT),// 4
			new Column("F_MEDIAENCODING", Types.SMALLINT),// 5
			new Column("F_EVENTTYPE", Types.SMALLINT),// 6
			new Column("F_CHANNEL", Types.SMALLINT),// 7
			new Column("F_EVENTTIME", Types.TIMESTAMP),// 8
			new Column("F_UPLOADTIME", Types.TIMESTAMP),// 9
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 10
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> mediaeventList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = mediaeventList.size(); i < size; i++) {
			try {
				MediaEvent mediaevent = MediaEvent.parseFrom(mediaeventList.get(i));
				s.add(buildSqlObjectListFromMediaEvent(mediaevent));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse MediaEvent error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromMediaEvent(MediaEvent mediaevent) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, mediaevent.getID());
		m.put(2, mediaevent.getVehicleID());
		m.put(3, mediaevent.getEventID());
		m.put(4, mediaevent.getMediaType());
		m.put(5, mediaevent.getMediaEncoding());
		m.put(6, mediaevent.getEventType());
		m.put(7, mediaevent.getChannel());
		m.put(8, new java.sql.Date(mediaevent.getEventTime()));
		m.put(9, new java.sql.Date(mediaevent.getUpLoadTime()));
		m.put(10, mediaevent.getEnterpriseCode());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}