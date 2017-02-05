package org.apache.storm.tuple;

/**
 * Created by PC on 2017/1/10.
 */
public interface OutputFieldsDeclarer {
    /**
     * Uses default stream id.
     */
    public void declare(Fields fields);
    public void declare(boolean direct, Fields fields);

    public void declareStream(String streamId, Fields fields);
    public void declareStream(String streamId, boolean direct, Fields fields);
}