package com.sxp.hbase.bolt.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.List;

public interface HBaseValueMapper extends Serializable {
	/**
	 *
	 * @param input
	 *            tuple.
	 * @param result
	 *            HBase lookup result instance.
	 * @return list of values that should be emitted by the lookup bolt.
	 * @throws Exception
	 */
	public List<Values> toValues(ITuple input, Result result) throws Exception;

	/**
	 * declares the output fields for the lookup bolt.
	 * 
	 * @param declarer
	 */
	void declareOutputFields(OutputFieldsDeclarer declarer);
}
