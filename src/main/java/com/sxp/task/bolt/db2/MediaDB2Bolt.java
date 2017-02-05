package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.MediaInfo.Media;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MediaDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(MediaDB2Bolt.class);
	static String TABLE_NAME = "T_MEDIA_INFO";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_EVENTID", Types.BIGINT),// 3
			new Column("F_MEDIATYPE", Types.SMALLINT),// 4
			new Column("F_MEDIAENCODING", Types.SMALLINT),// 5
			new Column("F_EVENTTYPE", Types.SMALLINT),// 6
			new Column("F_CHANNEL", Types.SMALLINT),// 7
			new Column("F_UPTIME", Types.TIMESTAMP),// 8
			new Column("F_UPTIMEEND", Types.TIMESTAMP),// 9
			new Column("F_FILEPATH", Types.VARCHAR, 200),// 10
			new Column("F_LOCATION_ID", Types.BIGINT),// 11
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 12
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> mediaList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = mediaList.size(); i < size; i++) {
			try {
				Media media = Media.parseFrom(mediaList.get(i));
				s.add(buildSqlObjectListFromMedia(media));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse Media error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromMedia(Media media) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, media.getID());
		m.put(2, media.getVehicleID());
		m.put(3, media.getEventID());
		m.put(4, media.getMediaType());
		m.put(5, media.getMediaEncoding());
		m.put(6, media.getEventType());
		m.put(7, media.getChannel());
		m.put(8, new java.sql.Date(media.getUpdateTime()));
		m.put(9, new java.sql.Date(media.getLastUpdate()));
		m.put(10, media.getPath());
		m.put(11, media.getLocationID());
		m.put(12, media.getEnterpriseCode());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}