package org.gooseman.hbase;

import org.apache.hadoop.hbase.client.Put;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class HBaseInputSet {

    private final Put put;

    public HBaseInputSet(Put put) {
        this.put = put;
    }

    public HBaseInputSet addString(String family, String column, String value) {
        put.addColumn(toBytes(family), toBytes(column), toBytes(value));
        return this;
    }

    public HBaseInputSet addDouble(String family, String column, Double value) {
        put.addColumn(toBytes(family), toBytes(column), toBytes(value));
        return this;
    }

    public HBaseInputSet addInteger(String family, String column, Integer value) {
        put.addColumn(toBytes(family), toBytes(column), toBytes(value));
        return this;
    }

    public HBaseInputSet addLong(String family, String column, Long value) {
        put.addColumn(toBytes(family), toBytes(column), toBytes(value));
        return this;
    }

    public HBaseInputSet addFloat(String family, String column, Float value) {
        put.addColumn(toBytes(family), toBytes(column), toBytes(value));
        return this;
    }

    public HBaseInputSet addBoolean(String family, String column, Boolean value) {
        put.addColumn(toBytes(family), toBytes(column), toBytes(value));
        return this;
    }

    public HBaseInputSet addShort(String family, String column, Short value) {
        put.addColumn(toBytes(family), toBytes(column), toBytes(value));
        return this;
    }

    public HBaseInputSet addBigDecimal(String family, String column, BigDecimal value) {
        put.addColumn(toBytes(family), toBytes(column), toBytes(value));
        return this;
    }

    public HBaseInputSet addLocalDate(String family, String column, LocalDate value) {
        put.addColumn(toBytes(family), toBytes(column), toBytes(value.toEpochDay()));
        return this;
    }

    public HBaseInputSet addLocalDateTime(String family, String column, LocalDateTime value) {
        addLocalDateTime(family, column, value, ZoneOffset.UTC);
        return this;
    }

    public HBaseInputSet addLocalDateTime(String family, String column, LocalDateTime value, ZoneOffset zoneOffset) {
        put.addColumn(toBytes(family), toBytes(column), toBytes(value.toInstant(zoneOffset).toEpochMilli()));
        return this;
    }

    Put getPut() {
        return this.put;
    }
}
