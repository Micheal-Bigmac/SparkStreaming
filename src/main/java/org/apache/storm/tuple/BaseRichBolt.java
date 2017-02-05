package org.apache.storm.tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by PC on 2017/1/10.
 */
public abstract class BaseRichBolt implements Serializable {
      public  List<Object> results=new ArrayList<Object>();
//    abstract void  declareOutputFields(OutputFieldsDeclarer declarer);

//    Map<String, Object> getComponentConfiguration();


//    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);


//     abstract  void execute(Tuple input);

//    void cleanup();
}
