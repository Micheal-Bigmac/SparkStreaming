package org.apache.storm.tuple;

import java.util.List;

/**
 * Created by PC on 2017/1/11.
 */
public class TupleImp implements Tuple {

    private Fields fields;
    private Values values;

    public TupleImp(Fields fields, Values values) {
        this.fields = fields;
        this.values = values;
    }

    @Override
    public String getSourceComponent() {
        return null;
    }

    @Override
    public int getSourceTask() {
        return 0;
    }

    @Override
    public String getSourceStreamId() {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean contains(String field) {
        return this.fields.contains(field);
    }

    @Override
    public Fields getFields() {
        return this.fields;
    }

    @Override
    public int fieldIndex(String field) {
        return this.fields.fieldIndex(field);
    }

    @Override
    public List<Object> select(Fields selector) {
        return this.fields.select(selector,values);
    }

    @Override
    public Object getValue(int i) {
        return this.values.get(i);
    }

    @Override
    public String getString(int i) {
        return (String)values.get(i);
    }

    @Override
    public Integer getInteger(int i) {
        return (Integer)values.get(i);
    }

    @Override
    public Long getLong(int i) {
        return (Long)values.get(i);
    }

    @Override
    public Boolean getBoolean(int i) {
        return (Boolean)values.get(i);
    }

    @Override
    public Short getShort(int i) {
        return (Short)values.get(i);
    }

    @Override
    public Byte getByte(int i) {
        return (Byte)values.get(i);
    }

    @Override
    public Double getDouble(int i) {
        return (Double)values.get(i);
    }

    @Override
    public Float getFloat(int i) {
        return (Float)values.get(i);
    }

    @Override
    public byte[] getBinary(int i) {
        return (byte[]) values.get(i);
    }

    @Override
    public Object getValueByField(String field) {
        return values.get(fieldIndex(field));
    }

    @Override
    public String getStringByField(String field) {
        return (String)values.get(fieldIndex(field));
    }

    @Override
    public Integer getIntegerByField(String field) {
        return (Integer)values.get(fieldIndex(field));
    }

    @Override
    public Long getLongByField(String field) {
        return (Long)values.get(fieldIndex(field));
    }

    @Override
    public Boolean getBooleanByField(String field) {
        return (Boolean)values.get(fieldIndex(field));
    }

    @Override
    public Short getShortByField(String field) {
        return (Short)values.get(fieldIndex(field));
    }

    @Override
    public Byte getByteByField(String field) {
        return (Byte)values.get(fieldIndex(field));
    }

    @Override
    public Double getDoubleByField(String field) {
        return (Double)values.get(fieldIndex(field));
    }

    @Override
    public Float getFloatByField(String field) {
        return (Float)values.get(fieldIndex(field));
    }

    @Override
    public byte[] getBinaryByField(String field) {
        return (byte[])values.get(fieldIndex(field));
    }

    @Override
    public List<Object> getValues() {
        return values;
    }
}
