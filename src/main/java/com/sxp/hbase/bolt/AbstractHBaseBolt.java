package com.sxp.hbase.bolt;
import com.sxp.hbase.bolt.mapper.HBaseMapper;
import com.sxp.hbase.common.HBaseClient;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.tuple.BaseRichBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

// TODO support more configuration options, for now we're defaulting to the hbase-*.xml files found on the classpath
public abstract class AbstractHBaseBolt extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseBolt.class);


	protected transient HBaseClient hBaseClient;
	protected String tableName;
	protected HBaseMapper mapper;
	protected String configKey;

	public AbstractHBaseBolt(String tableName, HBaseMapper mapper) {
		Validate.notEmpty(tableName, "Table name can not be blank or null");
		Validate.notNull(mapper, "mapper can not be null");
		this.tableName = tableName;
		this.mapper = mapper;
	}

	public void prepare(Map map) {
		final Configuration hbConfig = HBaseConfiguration.create();

		Map<String, Object> conf = (Map<String, Object>) map.get(this.configKey);
		if (conf == null) {
			throw new IllegalArgumentException("HBase configuration not found using key '" + this.configKey + "'");
		}
		if (conf.get("hbase.rootdir") == null) {
			LOG.warn("No 'hbase.rootdir' value found in configuration! Using HBase defaults.");
		}
		for (String key : conf.keySet()) {
			hbConfig.set(key, String.valueOf(conf.get(key)));
		}

		this.hBaseClient = new HBaseClient(conf, hbConfig, tableName);
	}
}
