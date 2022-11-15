package cn.xdf.acdc.devops.tool.http;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import okhttp3.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * OK http util.
 */
public class OkUtil {

    private static final Map<String, Object> DEFAULT_ERROR_BODY = new LinkedHashMap<>();

    private static final Map<String, Object> DEFAULT_SUCCESS_BODY = new LinkedHashMap<>();

    private static final Map<String, Object> BODY_401 = new LinkedHashMap<>();

    private static final Gson GSON = new GsonBuilder().create();

    private static final Set<String> IGNORE_KEY_SET = new HashSet();

    static {
        // special treatment
        IGNORE_KEY_SET.add("showType");
        IGNORE_KEY_SET.add("errorCode");
        IGNORE_KEY_SET.add("success");
        IGNORE_KEY_SET.add("user");
        //  fail
        DEFAULT_ERROR_BODY.put("errorMessage", "Bad Gateway");
        DEFAULT_ERROR_BODY.put("code", 502);

        // success
        DEFAULT_SUCCESS_BODY.put("code", 200);

        // 401
        BODY_401.put("errorMessage", "Unauthorized");
        BODY_401.put("code", 401);
    }

    /**
     * Determines whether the set is empty .
     *
     * @param collection java collection
     * @return true: empty, false: not empty
     */
    public static boolean isEmpty(final Collection collection) {
        return Objects.isNull(collection) || collection.isEmpty();
    }

    /**
     * Determines whether the set is empty .
     *
     * @param map java map
     * @return true: empty, false: not empty
     */
    public static boolean isEmpty(final Map map) {
        return Objects.isNull(map) || map.isEmpty();
    }

    private static boolean isEmpty(final String str) {
        return Objects.isNull(str) || "".equals(str) || str.length() <= 0;
    }

    /**
     * Determines whether the set is non-empty .
     *
     * @param collection java collection
     * @return true: empty, false: not empty
     */
    public static boolean notEmpty(final Collection collection) {
        return !isEmpty(collection);
    }

    /**
     * Determines whether the set is non-empty .
     *
     * @param map java map
     * @return true: empty, false: not empty
     */
    public static boolean notEmpty(final Map map) {
        return !(isEmpty(map));
    }

    /**
     * Gets the result of the http response, and assigns a default result if no result exists.
     *
     * @param response ok http response
     * @return json string
     * @throws IOException parse json error
     */
    public static String getResultWithDefault(final Response response) throws IOException {

        if (response.code() == 401) {
            return GSON.toJson(BODY_401);
        }

        if (Objects.isNull(response.body())) {
            if (response.isSuccessful()) {
                return GSON.toJson(DEFAULT_SUCCESS_BODY);
            } else {
                return GSON.toJson(DEFAULT_ERROR_BODY);
            }
        }

        String body = response.body().string();
        if (isEmpty(body)) {
            if (response.isSuccessful()) {
                return GSON.toJson(DEFAULT_SUCCESS_BODY);
            } else {
                return GSON.toJson(DEFAULT_ERROR_BODY);
            }
        }

        Map<String, Object> result = GSON.fromJson(body, Map.class);
        IGNORE_KEY_SET.forEach(result::remove);

        result.put("code", response.code());
        return GSON.toJson(result);
    }

    /**
     * Get json body.
     *
     * @param entity entity object
     * @return json body string
     */
    public static String getJsonBody(final Object entity) {
        return GSON.toJson(entity);
    }

}
