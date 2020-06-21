package org.gooseman.hbase;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
public class Pojo {
    private float floatPrimitive;
    private Float floatWrapper;
    private int intPrimitive;
    private Integer intWrapper;
    private double doublePrimitive;
    private long longPrimitive;
    private Long longWrapper;
    private Double doubleWrapper;
    private String string;
    private boolean booleanPrimitive;
    private Boolean booleanWrapper;
    private short shortPrimitive;
    private Short shortWrapper;
    private BigDecimal bigDecimal;
    private LocalDate localDate;
    private LocalDateTime localDateTime;

    public boolean getBooleanPrimitive() {
        return this.booleanPrimitive;
    }
}
