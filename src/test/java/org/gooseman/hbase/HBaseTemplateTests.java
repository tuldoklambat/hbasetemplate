package org.gooseman.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

public class HBaseTemplateTests {

    private final static String FAMILY = "d";
    private final static byte[] BIN_FAMILY = toBytes(FAMILY);

    private Pojo samplePojo;

    private Connection mockConnection;
    private Table mockTable;

    @Before
    public void setup() throws IOException {
        samplePojo = new Pojo();
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

        Result mockResult = mock(Result.class);
        when(mockResult.getValue(BIN_FAMILY, toBytes("floatPrimitive"))).thenReturn(toBytes(samplePojo.getFloatPrimitive()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("floatWrapper"))).thenReturn(toBytes(samplePojo.getFloatWrapper()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("intPrimitive"))).thenReturn(toBytes(samplePojo.getIntPrimitive()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("intWrapper"))).thenReturn(toBytes(samplePojo.getIntWrapper()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("intWrapper"))).thenReturn(toBytes(samplePojo.getIntWrapper()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("longPrimitive"))).thenReturn(toBytes(samplePojo.getLongPrimitive()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("longWrapper"))).thenReturn(toBytes(samplePojo.getLongWrapper()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("doublePrimitive"))).thenReturn(toBytes(samplePojo.getDoublePrimitive()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("doubleWrapper"))).thenReturn(toBytes(samplePojo.getDoubleWrapper()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("string"))).thenReturn(toBytes(samplePojo.getString()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("boolPrimitive"))).thenReturn(toBytes(samplePojo.getBooleanPrimitive()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("boolWrapper"))).thenReturn(toBytes(samplePojo.getBooleanWrapper()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("shortPrimitive"))).thenReturn(toBytes(samplePojo.getShortPrimitive()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("shortWrapper"))).thenReturn(toBytes(samplePojo.getShortWrapper()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("bigDecimal"))).thenReturn(toBytes(samplePojo.getBigDecimal()));
        when(mockResult.getValue(BIN_FAMILY, toBytes("localDate"))).thenReturn(toBytes(samplePojo.getLocalDate().toEpochDay()));
        // let's assume the local date time saved in HBase was saved using UTC timezone
        when(mockResult.getValue(BIN_FAMILY, toBytes("localDateTime"))).thenReturn(toBytes(samplePojo.getLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli()));

        Iterator<Result> mockResultIterator = mock(Iterator.class);
        when(mockResultIterator.hasNext()).thenReturn(true).thenReturn(false);
        when(mockResultIterator.next()).thenReturn(mockResult);

        ResultScanner mockResultScanner = mock(ResultScanner.class);
        when(mockResultScanner.iterator()).thenReturn(mockResultIterator);

        mockTable = mock(Table.class);
        when(mockTable.get(any(Get.class))).thenReturn(mockResult);
        when(mockTable.get(anyList())).thenReturn(new Result[] { mockResult });
        when(mockTable.getScanner(any(Scan.class))).thenReturn(mockResultScanner);
        doNothing().when(mockTable).put(anyList());

        mockConnection = mock(Connection.class);
        when(mockConnection.getTable(any(TableName.class))).thenReturn(mockTable);
    }

    @Test
    public void get_should_return_the_correct_values() throws IOException {
        HBaseTemplate hBaseTemplate = new HBaseTemplate(mockConnection);
        Pojo pojo = hBaseTemplate.get("mockHbaseTable", new Get(toBytes("mockRowKey")), this::rowMapper);

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

    @Test
    public void gets_should_return_the_correct_values() throws IOException {
        List<Pojo> pojos = new ArrayList<>();
        HBaseTemplate hBaseTemplate = new HBaseTemplate(mockConnection);
        hBaseTemplate.get("mockHbaseTable", Collections.singletonList(new Get(toBytes("mockRowKey"))), this::rowMapper)
                .forEach(pojos::add);

        Assert.assertEquals(samplePojo.getFloatPrimitive(), pojos.get(0).getFloatPrimitive(), 0.0);
        Assert.assertEquals(samplePojo.getFloatWrapper(), pojos.get(0).getFloatWrapper(), 0.0);
        Assert.assertEquals(samplePojo.getIntPrimitive(), pojos.get(0).getIntPrimitive());
        Assert.assertEquals(samplePojo.getIntWrapper(), pojos.get(0).getIntWrapper());
        Assert.assertEquals(samplePojo.getLongPrimitive(), pojos.get(0).getLongPrimitive());
        Assert.assertEquals(samplePojo.getLongWrapper(), pojos.get(0).getLongWrapper());
        Assert.assertEquals(samplePojo.getDoublePrimitive(), pojos.get(0).getDoublePrimitive(), 0.0);
        Assert.assertEquals(samplePojo.getDoubleWrapper(), pojos.get(0).getDoubleWrapper(), 0.0);
        Assert.assertEquals(samplePojo.getString(), pojos.get(0).getString());
        Assert.assertEquals(samplePojo.getBooleanPrimitive(), pojos.get(0).getBooleanPrimitive());
        Assert.assertEquals(samplePojo.getBooleanWrapper(), pojos.get(0).getBooleanWrapper());
        Assert.assertEquals(samplePojo.getShortPrimitive(), pojos.get(0).getShortPrimitive());
        Assert.assertEquals(samplePojo.getShortWrapper(), pojos.get(0).getShortWrapper());
        Assert.assertEquals(samplePojo.getBigDecimal(), pojos.get(0).getBigDecimal());
        Assert.assertEquals(samplePojo.getLocalDate(), pojos.get(0).getLocalDate());
        Assert.assertEquals(samplePojo.getLocalDateTime(), pojos.get(0).getLocalDateTime());
    }

    @Test
    public void scan_should_return_the_correct_values() throws IOException {
        List<Pojo> pojos = new ArrayList<>();
        HBaseTemplate hBaseTemplate = new HBaseTemplate(mockConnection);
        for(Iterator<Pojo> iterator = hBaseTemplate.scan("mockHbaseTable", new Scan(), this::rowMapper); iterator.hasNext();) {
            pojos.add(iterator.next());
        }
        Assert.assertEquals(samplePojo.getFloatPrimitive(), pojos.get(0).getFloatPrimitive(), 0.0);
        Assert.assertEquals(samplePojo.getFloatWrapper(), pojos.get(0).getFloatWrapper(), 0.0);
        Assert.assertEquals(samplePojo.getIntPrimitive(), pojos.get(0).getIntPrimitive());
        Assert.assertEquals(samplePojo.getIntWrapper(), pojos.get(0).getIntWrapper());
        Assert.assertEquals(samplePojo.getLongPrimitive(), pojos.get(0).getLongPrimitive());
        Assert.assertEquals(samplePojo.getLongWrapper(), pojos.get(0).getLongWrapper());
        Assert.assertEquals(samplePojo.getDoublePrimitive(), pojos.get(0).getDoublePrimitive(), 0.0);
        Assert.assertEquals(samplePojo.getDoubleWrapper(), pojos.get(0).getDoubleWrapper(), 0.0);
        Assert.assertEquals(samplePojo.getString(), pojos.get(0).getString());
        Assert.assertEquals(samplePojo.getBooleanPrimitive(), pojos.get(0).getBooleanPrimitive());
        Assert.assertEquals(samplePojo.getBooleanWrapper(), pojos.get(0).getBooleanWrapper());
        Assert.assertEquals(samplePojo.getShortPrimitive(), pojos.get(0).getShortPrimitive());
        Assert.assertEquals(samplePojo.getShortWrapper(), pojos.get(0).getShortWrapper());
        Assert.assertEquals(samplePojo.getBigDecimal(), pojos.get(0).getBigDecimal());
        Assert.assertEquals(samplePojo.getLocalDate(), pojos.get(0).getLocalDate());
        Assert.assertEquals(samplePojo.getLocalDateTime(), pojos.get(0).getLocalDateTime());
    }

    @Test
    public void save_entity() throws IOException {
        HBaseTemplate hBaseTemplate = new HBaseTemplate(mockConnection);
        hBaseTemplate.save("mockHbaseTable", samplePojo, this::putMapper);

        // verify table put was called
        verify(mockTable, times(1)).put(anyList());
    }

    @Test
    public void save_entities() throws IOException {
        HBaseTemplate hBaseTemplate = new HBaseTemplate(mockConnection);
        hBaseTemplate.save("mockHbaseTable", Collections.singletonList(samplePojo), this::putMapper);

        // verify table put was called
        verify(mockTable, times(1)).put(anyList());
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
                .addFloat(FAMILY, "floatPrimitive", pojo.getFloatWrapper())
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
