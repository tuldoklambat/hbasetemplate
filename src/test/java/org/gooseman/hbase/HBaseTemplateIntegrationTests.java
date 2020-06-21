package org.gooseman.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class HBaseTemplateIntegrationTests {

    private final static String FAMILY = "d";
    private HBaseTemplate hBaseTemplate;

    @Before
    public void setup() throws IOException {
        Configuration hBaseConfig =  HBaseConfiguration.create();
        hBaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
        hBaseConfig.set("hbase.zookeeper.quorom", "127.0.0.1");

        hBaseTemplate = new HBaseTemplate(ConnectionFactory.createConnection(hBaseConfig));
    }

    @Test
    public void testSaveAndRetrieve() throws IOException {
        Pojo samplePojo = new Pojo();
        samplePojo.setFloatPrimitive(Float.MIN_VALUE);
        samplePojo.setFloatWrapper(Float.MAX_VALUE);
        samplePojo.setIntPrimitive(Integer.MIN_VALUE);
        samplePojo.setIntWrapper(Integer.MAX_VALUE);
        samplePojo.setLongPrimitive(Long.MIN_VALUE);
        samplePojo.setLongWrapper(Long.MAX_VALUE);
        samplePojo.setDoublePrimitive(Double.MIN_NORMAL);
        samplePojo.setDoubleWrapper(Double.MAX_VALUE);
        samplePojo.setString("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.");
        samplePojo.setBooleanPrimitive(Boolean.TRUE);
        samplePojo.setBooleanWrapper(Boolean.FALSE);
        samplePojo.setShortPrimitive(Short.MIN_VALUE);
        samplePojo.setShortWrapper(Short.MAX_VALUE);
        samplePojo.setBigDecimal(BigDecimal.TEN);
        samplePojo.setLocalDate(LocalDate.MAX);
        samplePojo.setLocalDateTime(LocalDateTime.of(2020, 1, 28, 6, 30, 15));

        hBaseTemplate.save("test", samplePojo, this::putMapper);

        Pojo pojo = hBaseTemplate.get("test", new Get(toBytes("mockRowKey")), this::rowMapper);

        Assert.assertEquals(samplePojo.getFloatPrimitive(), pojo.getFloatPrimitive(), 0.0);
        Assert.assertEquals(samplePojo.getFloatWrapper(), pojo.getFloatWrapper(), 0.0);
        Assert.assertEquals(samplePojo.getIntPrimitive(), pojo.getIntPrimitive());
        Assert.assertEquals(samplePojo.getIntWrapper(), pojo.getIntWrapper());
        Assert.assertEquals(samplePojo.getLongPrimitive(), pojo.getLongPrimitive());
        Assert.assertEquals(samplePojo.getLongWrapper(), pojo.getLongWrapper());
        Assert.assertEquals(samplePojo.getDoublePrimitive(), pojo.getDoublePrimitive(), 0.0);
        Assert.assertEquals(samplePojo.getDoubleWrapper(), pojo.getDoubleWrapper(), 0.0);
        Assert.assertEquals(samplePojo.getString(), pojo.getString());
        Assert.assertEquals(samplePojo.getBooleanPrimitive(), pojo.getBooleanPrimitive());
        Assert.assertEquals(samplePojo.getBooleanWrapper(), pojo.getBooleanWrapper());
        Assert.assertEquals(samplePojo.getShortPrimitive(), pojo.getShortPrimitive());
        Assert.assertEquals(samplePojo.getShortWrapper(), pojo.getShortWrapper());
        Assert.assertEquals(samplePojo.getBigDecimal(), pojo.getBigDecimal());
        Assert.assertEquals(samplePojo.getLocalDate(), pojo.getLocalDate());
        Assert.assertEquals(samplePojo.getLocalDateTime(), pojo.getLocalDateTime());
    }

    private Pojo rowMapper(HBaseOutputSet outputSet) {
        Pojo pojo = new Pojo();
        pojo.setFloatPrimitive(outputSet.getFloat(FAMILY, "floatPrimitive"));
        pojo.setFloatWrapper(outputSet.getFloat(FAMILY, "floatWrapper"));
        pojo.setIntPrimitive(outputSet.getInteger(FAMILY, "intPrimitive"));
        pojo.setIntWrapper(outputSet.getInteger(FAMILY, "intWrapper"));
        pojo.setLongPrimitive(outputSet.getLong(FAMILY, "longPrimitive"));
        pojo.setLongWrapper(outputSet.getLong(FAMILY, "longWrapper"));
        pojo.setDoublePrimitive(outputSet.getDouble(FAMILY, "doublePrimitive"));
        pojo.setDoubleWrapper(outputSet.getDouble(FAMILY, "doubleWrapper"));
        pojo.setString(outputSet.getString(FAMILY, "string"));
        pojo.setBooleanPrimitive(outputSet.getBoolean(FAMILY, "boolPrimitive"));
        pojo.setBooleanWrapper(outputSet.getBoolean(FAMILY, "boolWrapper"));
        pojo.setShortPrimitive(outputSet.getShort(FAMILY, "shortPrimitive"));
        pojo.setShortWrapper(outputSet.getShort(FAMILY, "shortWrapper"));
        pojo.setBigDecimal(outputSet.getBigDecimal(FAMILY, "bigDecimal"));
        pojo.setLocalDate(outputSet.getLocalDate(FAMILY, "localDate"));
        pojo.setLocalDateTime(outputSet.getLocalDateTime(FAMILY, "localDateTime", ZoneOffset.UTC));
        return pojo;
    }

    private HBaseInputSet putMapper(Pojo pojo) {
        return new HBaseInputSet(new Put(toBytes("mockRowKey")))
                .addFloat(FAMILY, "floatPrimitive", pojo.getFloatPrimitive())
                .addFloat(FAMILY, "floatWrapper", pojo.getFloatWrapper())
                .addInteger(FAMILY, "intPrimitive", pojo.getIntPrimitive())
                .addInteger(FAMILY, "intWrapper", pojo.getIntWrapper())
                .addLong(FAMILY, "longPrimitive", pojo.getLongPrimitive())
                .addLong(FAMILY, "longWrapper", pojo.getLongWrapper())
                .addDouble(FAMILY, "doublePrimitive", pojo.getDoublePrimitive())
                .addDouble(FAMILY, "doubleWrapper", pojo.getDoubleWrapper())
                .addString(FAMILY, "string", pojo.getString())
                .addBoolean(FAMILY, "boolPrimitive", pojo.getBooleanPrimitive())
                .addBoolean(FAMILY, "boolWrapper", pojo.getBooleanWrapper())
                .addShort(FAMILY, "shortPrimitive", pojo.getShortPrimitive())
                .addShort(FAMILY, "shortWrapper", pojo.getShortWrapper())
                .addBigDecimal(FAMILY, "bigDecimal", pojo.getBigDecimal())
                .addLocalDate(FAMILY, "localDate", pojo.getLocalDate())
                .addLocalDateTime(FAMILY, "localDateTime", pojo.getLocalDateTime());
    }
}
