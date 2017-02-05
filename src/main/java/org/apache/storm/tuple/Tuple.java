package org.apache.storm.tuple;

/**
 * Created by PC on 2017/1/10.
 */
public interface Tuple extends ITuple {

    public String getSourceComponent();

    /**
     * Gets the id of the task that created this tuple.
     */
    public int getSourceTask();

    /**
     * Gets the id of the stream that this tuple was emitted to.
     */
    public String getSourceStreamId();
}
