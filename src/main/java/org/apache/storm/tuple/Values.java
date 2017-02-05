package org.apache.storm.tuple;

import java.util.ArrayList;

/**
 * Created by PC on 2017/1/10.
 */
public class Values extends ArrayList<Object> {
    public Values() {

    }

    public Values(Object... vals) {
        super(vals.length);
        for(Object o: vals) {
            add(o);
        }
    }
}
