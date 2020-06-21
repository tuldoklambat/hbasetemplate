package org.gooseman.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HBaseTemplate implements Closeable {

    private final Connection connection;

    public HBaseTemplate(Connection connection) {
        this.connection = connection;
    }

    public <T> T get(String tableName, Get get, Function<HBaseOutputSet, T> rowMapper) throws IOException {
        return rowMapper.apply(new HBaseOutputSet(connection.getTable(TableName.valueOf(tableName)).get(get)));
    }

    public <T> Stream<T> get(String tableName, List<Get> gets, Function<HBaseOutputSet, T> rowMapper) throws IOException {
        Result[] results = connection.getTable(TableName.valueOf(tableName)).get(gets);
        return Arrays.stream(results).map(result -> rowMapper.apply(new HBaseOutputSet(result)));
    }

    public <T> Iterator<T> scan(String tableName, Scan scan, Function<HBaseOutputSet, T> rowMapper) throws IOException {
        try(ResultScanner results = connection.getTable(TableName.valueOf(tableName)).getScanner(scan)) {
            return new Iterator<T>() {
                @Override
                public boolean hasNext() {
                    return results.iterator().hasNext();
                }

                @Override
                public T next() {
                    return rowMapper.apply(new HBaseOutputSet(results.iterator().next()));
                }
            };
        }
    }

    public <T> void save(String tableName, T entity, Function<T, HBaseInputSet> putMapper) throws IOException {
        save(tableName, Collections.singletonList(entity), putMapper);
    }

    public <T> void save(String tableName, List<T> entities, Function<T, HBaseInputSet> putMapper) throws IOException {
        List<Put> puts =  entities.stream().map(putMapper).map(HBaseInputSet::getPut).collect(Collectors.toList());
        connection.getTable(TableName.valueOf(tableName)).put(puts);
    }

    public void close() throws IOException {
        connection.close();
    }
}
