package cn.xdf.acdc.connect.core.util;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

public class ExceptionUtilsTest {
    
    private SQLException originException;
    
    private final int maxLevel = 5;
    
    @Before
    public void setUp() throws Exception {
        SQLException head = new SQLException("level: " + 0);
        SQLException current = head;
        for (int i = 1; i < maxLevel; i++) {
            SQLException next = new SQLException("level: " + i);
            current.setNextException(next);
            current = next;
        }
        originException = head;
    }
    
    @Test
    public void shouldGetRetriableException() {
        RetriableException exception = ExceptionUtils.parseToFlatMessageRetriableException(originException);
        Assert.assertEquals(maxLevel + 1, exception.getMessage().split(System.lineSeparator()).length);
    }
    
    @Test
    public void shouldGetConnectException() {
        ConnectException exception = ExceptionUtils.parseToFlatMessageConnectException(originException);
        Assert.assertEquals(maxLevel + 1, exception.getMessage().split(System.lineSeparator()).length);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentException() {
        ExceptionUtils.parseToFlatMessageConnectException(new ConnectException("wrong type exception for argument"));
    }
    
}
