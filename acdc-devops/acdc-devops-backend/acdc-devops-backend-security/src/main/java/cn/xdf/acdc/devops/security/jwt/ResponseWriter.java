package cn.xdf.acdc.devops.security.jwt;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public class ResponseWriter {

    /**
     * 登录结果返回给客户端.
     *
     * @param response response
     * @param content  content
     * @param status   http status
     * @throws IOException exception
     */
    public static void write(final HttpServletResponse response, final String content, final HttpStatus status) throws IOException {
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setStatus(status.value());
        PrintWriter printWriter = response.getWriter();
        printWriter.write(content);
    }
}
