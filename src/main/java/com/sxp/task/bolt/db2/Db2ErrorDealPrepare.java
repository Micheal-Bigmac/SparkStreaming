package com.sxp.task.bolt.db2;

import com.hsae.rdbms.db2.DB2ExcutorQuenue;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import org.apache.storm.tuple.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Db2ErrorDealPrepare extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(CanDB2Bolt.class);
	private static Connection connection = null;
	public void prepare(Map stormConf) {
		connection = DB2ExcutorQuenue.isExstConnection(connection);
	}

	public void execute(Tuple input) {
		List<PreparedSqlAndValues> sqls = (List<PreparedSqlAndValues>) input.getValue(0);
		List<PreparedSqlAndValues> onePerone =new ArrayList<PreparedSqlAndValues>();
		if(connection==null){
			LOG.error("未获取数据库  ====");
			LOG.error("未获取数据库  ====");
		}
		
		for(PreparedSqlAndValues tmp : sqls){
			try {
				onePerone.add(tmp);
				DB2ExcutorQuenue.excuteBatchs(onePerone,connection);
			} catch (SQLException e) {
			}finally{
				onePerone.clear();
			}
		}
	}
}
