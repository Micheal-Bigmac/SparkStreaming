package com.hsae.rdbms.db2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hsae.rdbms.common.PropertiesUtil;
import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

public class DB2ConnectionPool {
	private final static Logger LOG = LoggerFactory.getLogger(DB2ConnectionPool.class);

	private static DB2ConnectionPool instance = null;
	private String uid = null;
	private String pwd = null;
	private String jdbcClassName = null;
	private String jdbcurl = null;

	BoneCP connectionPool = null;

	public static DB2ConnectionPool getInstance() {
		if (instance == null) {
			instance = new DB2ConnectionPool();
			LOG.info("inited db2 connection pool");
		}
		return instance;
	}

	public DB2ConnectionPool() {
		if (connectionPool == null) {
			initConnectionPool();
		}
	}

	public Connection getConnection() throws SQLException {
		LOG.debug("Connection Pool TotalCreatedConnections=" + connectionPool.getTotalCreatedConnections() + ", TotalFree=" + connectionPool.getTotalFree() + ", TotalLeased="
				+ connectionPool.getTotalLeased());
		return connectionPool.getConnection();
	}

	public void initConnectionPool() {
		Properties prop = PropertiesUtil.getProperites("database-db2.properties");
		jdbcClassName = prop.getProperty("jdbc.driverClassName");
		jdbcurl = prop.getProperty("jdbc.url");
		uid = prop.getProperty("jdbc.username");
		pwd = prop.getProperty("jdbc.password");

		try {
			// 加载JDBC驱动
			Class.forName(jdbcClassName);
			// 设置连接池配置信息
			BoneCPConfig config = new BoneCPConfig();
			// 数据库的JDBC URL
			config.setJdbcUrl(jdbcurl);
			// 数据库用户名
			config.setUsername(uid);
			// 数据库用户密码
			config.setPassword(pwd);
			// 下面是连接池的配置
			// #分区数量
			int partitionCount = Integer.parseInt(prop.getProperty("bonecp.partitionCount"));
			config.setPartitionCount(partitionCount);
			// #每个分区含有的最小连接数
			int minConnectionsPerPartition = Integer.parseInt(prop.getProperty("bonecp.minConnectionsPerPartition"));
			config.setMinConnectionsPerPartition(minConnectionsPerPartition);
			// #每个分区含有的最大连接数
			int maxConnectionsPerPartition = Integer.parseInt(prop.getProperty("bonecp.maxConnectionsPerPartition"));
			config.setMaxConnectionsPerPartition(maxConnectionsPerPartition);
			// #每次新增连接的数量
			int acquireIncrement = Integer.parseInt(prop.getProperty("bonecp.acquireIncrement"));
			config.setAcquireIncrement(acquireIncrement);
			// #连接池阀值，当 可用连接/最大连接 < 连接阀值 时，创建新的连接
			int poolAvailabilityThreshold = Integer.parseInt(prop.getProperty("bonecp.poolAvailabilityThreshold"));
			config.setPoolAvailabilityThreshold(poolAvailabilityThreshold);
			// #连接超时时间阀值，获取连接时，超出阀值时间，则获取失败，毫秒为单位
			int connectionTimeoutInMs = Integer.parseInt(prop.getProperty("bonecp.connectionTimeoutInMs"));
			config.setConnectionTimeoutInMs(connectionTimeoutInMs);
			// #测试连接有效性的间隔时间，单位分钟
			int idleConnectionTestPeriodInMinutes = Integer.parseInt(prop.getProperty("bonecp.idleConnectionTestPeriodInMinutes"));
			config.setIdleConnectionTestPeriodInMinutes(idleConnectionTestPeriodInMinutes);
			// #连接的空闲存活时间，当连接空闲时间大于该阀值时，清除该连接
			int idleMaxAgeInMinutes = Integer.parseInt(prop.getProperty("bonecp.idleMaxAgeInMinutes"));
			config.setIdleMaxAgeInMinutes(idleMaxAgeInMinutes);
			// #语句缓存个数，默认是0
			int statementsCacheSize = Integer.parseInt(prop.getProperty("bonecp.statementsCacheSize"));
			config.setStatementsCacheSize(statementsCacheSize);
			// #If true, track statements and close them if application forgot
			// to do so.
			boolean closeOpenStatements = Boolean.parseBoolean(prop.getProperty("bonecp.closeOpenStatements"));
			config.setCloseOpenStatements(closeOpenStatements);
			// #If true, print out a stack trace of where a statement was opened
			// but not closed before the connection was closed.
			boolean detectUnclosedStatements = Boolean.parseBoolean(prop.getProperty("bonecp.detectUnclosedStatements"));
			config.setDetectUnclosedStatements(detectUnclosedStatements);
			config.setDisableConnectionTracking(true);
			// 设置数据库连接池
			connectionPool = new BoneCP(config);
		} catch (Exception e) {
			LOG.error("BoneCP error! ", e);
		}
	}

}
