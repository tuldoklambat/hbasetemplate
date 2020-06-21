/*
 * Copyright (c) 2020 Gooseman Brothers (gooseman.brothers@gmail.com)
 *  All rights reserved.
 *
 *  THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
 *  WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
 *  MERCHANTABILITY OR NON-INFRINGEMENT.
 */

package org.gooseman.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class HBaseOutputSet {

    private final Result result;

    public HBaseOutputSet(Result result) {
        this.result = result;
    }

    public String getString(String family, String column) {
       return Bytes.toString(result.getValue(toBytes(family), toBytes(column)));
    }

    public Double getDouble(String family, String column) {
        return Bytes.toDouble(result.getValue(toBytes(family), toBytes(column)));
    }

    public Integer getInteger(String family, String column) {
        return Bytes.toInt(result.getValue(toBytes(family), toBytes(column)));
    }

    public Long getLong(String family, String column) {
        return Bytes.toLong(result.getValue(toBytes(family), toBytes(column)));
    }

    public Float getFloat(String family, String column) {
        return Bytes.toFloat(result.getValue(toBytes(family), toBytes(column)));
    }

    public Boolean getBoolean(String family, String column) {
        return Bytes.toBoolean(result.getValue(toBytes(family), toBytes(column)));
    }

    public Short getShort(String family, String column) {
        return Bytes.toShort(result.getValue(toBytes(family), toBytes(column)));
    }

    public BigDecimal getBigDecimal(String family, String column) {
        return Bytes.toBigDecimal(result.getValue(toBytes(family), toBytes(column)));
    }

    public LocalDate getLocalDate(String family, String column) {
        return LocalDate.ofEpochDay(Bytes.toLong(result.getValue(toBytes(family), toBytes(column))));
    }

    public LocalDateTime getLocalDateTime(String family, String column) {
        return  getLocalDateTime(family, column, ZoneOffset.UTC);
    }

    public LocalDateTime getLocalDateTime(String family, String column, ZoneOffset zoneOffset) {
        return  LocalDateTime.ofInstant(Instant.ofEpochMilli(Bytes.toLong(result.getValue(toBytes(family), toBytes(column)))), zoneOffset);
    }
}
