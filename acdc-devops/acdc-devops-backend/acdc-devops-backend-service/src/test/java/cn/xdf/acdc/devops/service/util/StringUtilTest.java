package cn.xdf.acdc.devops.service.util;

import cn.xdf.acdc.devops.core.util.StringUtil;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class StringUtilTest {
    
    @Test
    public void testConvertJsonToMapShouldPassWhenStringIsJsonFormat() {
        Map<String, String> result = StringUtil.convertJsonStringToMap("{\"a\":\"a\", \"b\":\"b\"}");
        
        Assertions.assertThat(result.size()).isEqualTo(2);
        Assertions.assertThat(result.get("a")).isEqualTo("a");
        Assertions.assertThat(result.get("b")).isEqualTo("b");
    }
}
