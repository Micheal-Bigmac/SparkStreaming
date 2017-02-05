package com.sxp.task.bolt.hbase.mapper;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.List;

/**
 * Maps a <code>backtype.task.tuple.Tuple</code> object to a row in an HBase table.
 */
public abstract class AbstractMapper implements Serializable {

    /**
     * Given a tuple, return a list of HBase columns to insert.
     *
     * @param tuple
     * @return
     */
    public abstract List<Mutation> mutations(Tuple tuple);

    /**
     * @param input  tuple.
     * @param result HBase lookup result instance.
     * @return list of values that should be emitted by the lookup bolt.
     * @throws Exception
     */
    public abstract List<Values> toValues(ITuple input, Result result) throws Exception;

    /**
     * declares the output fields for the lookup bolt.
     *
     * @param declarer
     * @throws NoSuchMethodException
     */
    abstract void declareOutputFields(OutputFieldsDeclarer declarer);


}
