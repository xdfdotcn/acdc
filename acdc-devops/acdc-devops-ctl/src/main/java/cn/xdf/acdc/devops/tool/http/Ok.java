package cn.xdf.acdc.devops.tool.http;

import okhttp3.ConnectionPool;
import okhttp3.FormBody;
import okhttp3.FormBody.Builder;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * OK http client.
 */
public class Ok {

    private static final int CONNECTION_TIMEOUT = 5;

    private static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");

    private static final int MAX_IDLE_CONNECTIONS = 5;

    private static final long KEEP_ALIVE_DURATION = 5L;

    private static final OkHttpClient HTTP_CLIENT = new OkHttpClient.Builder()
            .connectTimeout(CONNECTION_TIMEOUT, TimeUnit.SECONDS)
            .connectionPool(new ConnectionPool(MAX_IDLE_CONNECTIONS, KEEP_ALIVE_DURATION, TimeUnit.MINUTES))
            .build();

    /**
     * Get Request.
     *
     * @param host          access address
     * @param resourcesPath resources path
     * @param params        request parameters
     * @param headers       request headers
     * @return response result
     * @throws IOException json parse error
     */
    public static String get(
            final String host,
            final String resourcesPath,
            final Map<String, Object> params,
            final Map<String, Object> headers
    ) throws IOException {
        String url = gertResourceUrl(host, resourcesPath);

        HttpUrl.Builder urlBuilder = HttpUrl.parse(url).newBuilder();
        addQueryParams(urlBuilder, params);

        Request.Builder reqBuild = new Request.Builder();
        addHeader(reqBuild, headers);

        Request request = reqBuild.url(urlBuilder.build()).get().build();

        // synchronous execution
        Response response = HTTP_CLIENT.newCall(request).execute();
        return OkUtil.getResultWithDefault(response);
    }

    /**
     * Post Request.
     *
     * @param host          access address
     * @param resourcesPath resources path
     * @param entity        request body
     * @param headers       request headers
     * @return response result
     * @throws IOException json parse error
     */
    public static String post(
            final String host,
            final String resourcesPath,
            final Object entity,
            final Map<String, Object> headers

    ) throws IOException {
        String url = gertResourceUrl(host, resourcesPath);

        String json = OkUtil.getJsonBody(entity);
        RequestBody body = RequestBody.create(MEDIA_TYPE_JSON, json);

        Request.Builder builder = new Request.Builder();
        addHeader(builder, headers);

        Request request = builder.url(url).post(body).build();

        Response response = HTTP_CLIENT.newCall(request).execute();
        return OkUtil.getResultWithDefault(response);
    }

    /**
     * Post Request.
     *
     * @param host          access address
     * @param resourcesPath resources path
     * @param params        request params
     * @param headers       request headers
     * @return response result
     * @throws IOException json parse error
     */
    public static String post(
            final String host,
            final String resourcesPath,
            final Map<String, Object> params,
            final Map<String, Object> headers

    ) throws IOException {
        RequestBody formBody = createFormBody(params);
        String url = gertResourceUrl(host, resourcesPath);

        Request.Builder builder = new Request.Builder();
        addHeader(builder, headers);

        Request request = builder.url(url).post(formBody).build();

        Response response = HTTP_CLIENT.newCall(request).execute();
        return OkUtil.getResultWithDefault(response);
    }

    private static void addHeader(final Request.Builder builder, final Map<String, Object> headers) {
        if (OkUtil.notEmpty(headers)) {
            headers.forEach((k, v) -> {
                if (Objects.nonNull(k) && Objects.nonNull(v)) {
                    builder.addHeader(k, String.valueOf(v));
                }
            });
        }
    }

    private static void addQueryParams(final HttpUrl.Builder urlBuilder, final Map<String, Object> params) {
        if (OkUtil.notEmpty(params)) {
            params.forEach((k, v) -> {
                if (Objects.nonNull(k) && Objects.nonNull(v)) {
                    urlBuilder.addQueryParameter(k, String.valueOf(v));
                }
            });
        }
    }

    private static RequestBody createFormBody(final Map<String, Object> params) {
        Builder builder = new FormBody.Builder();
        if (OkUtil.notEmpty(params)) {
            params.forEach((k, v) -> {
                if (Objects.nonNull(k) && Objects.nonNull(v)) {
                    builder.add(k, String.valueOf(v));
                }
            });
        }
        return builder.build();
    }

    private static String gertResourceUrl(final String host, final String resourcesPath) {
        return new StringBuilder().append(host).append(resourcesPath).toString();
    }
}
