package cn.xdf.acdc.devops.core.util;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class StringUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Converts set to string.
     *
     * @param set       set
     * @param separator separator
     * @return string
     */
    public static String convertSetToStringWithSeparator(final Set<String> set, final String separator) {
        if (CollectionUtils.isEmpty(set)) {
            return SystemConstant.EMPTY_STRING;
        }

        return Joiner.on(separator).join(set);
    }

    /**
     * Converts string set.
     *
     * @param str       string
     * @param separator separator
     * @return set
     */
    public static Set<String> convertStringToSetWithSeparator(final String str, final String separator) {
        if (Strings.isNullOrEmpty(str)) {
            return Collections.EMPTY_SET;
        }
        return new HashSet<>(Splitter.on(separator).splitToList(str));
    }

    /**
     * Convert a json format string to map.
     *
     * @param jsonStr string in json format
     * @return map
     */
    public static Map<String, String> convertJsonStringToMap(final String jsonStr) {
        try {
            return OBJECT_MAPPER.readValue(jsonStr, Map.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("error when convert string to map, check if it is in json format", e);
        }
    }

    /**
     * If equals ignore case, return true, else return false.
     *
     * @param str1 str1
     * @param str2 str2
     * @return true if strings are equal, otherwise false
     */
    public static boolean equalsIgnoreCase(final String str1, final String str2) {
        String newStr1 = Objects.isNull(str1) ? SystemConstant.EMPTY_STRING : str1;
        String newStr2 = Objects.isNull(str2) ? SystemConstant.EMPTY_STRING : str2;
        return newStr1.equalsIgnoreCase(newStr2);
    }
}
