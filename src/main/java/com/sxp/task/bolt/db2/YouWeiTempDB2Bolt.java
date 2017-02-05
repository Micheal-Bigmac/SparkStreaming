package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.common.CommonTools;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.YouWei.YouWeiTemp;
import com.sxp.task.protobuf.generated.YouWei.YouWeiTempD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YouWeiTempDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(YouWeiTempDB2Bolt.class);
	static String TABLE_NAME = "T_LOCATION_HISTORY_INF_NUMBER_EXT";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_LOCATION_ID", Types.BIGINT),// 2
			new Column("F_APPEND_ID", Types.BIGINT),// 3
			new Column("F_APPEND_VALUE", Types.DOUBLE),// 4
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 5
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> youweitempList) throws InvalidProtocolBufferException {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = youweitempList.size(); i < size; i++) {
			try {
				YouWeiTemp youweitemp = YouWeiTemp.parseFrom(youweitempList.get(i));
				s.addAll(buildSqlObjectMapFromYouWeiTemp(youweitemp));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse YouWeiTemp error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static List<Map<Integer, Object>> buildSqlObjectMapFromYouWeiTemp(YouWeiTemp youweitemp) {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<YouWeiTempD> items = youweitemp.getItemsList();
		for (YouWeiTempD youWeiTempD : items) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, CommonTools.getPK());
			m.put(2, youweitemp.getLocationID());
			m.put(3, youWeiTempD.getTempID());
			m.put(4, youWeiTempD.getTempValue());
			m.put(5, youweitemp.getEnterpriseCode());
			s.add(m);
		}
		return s;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}