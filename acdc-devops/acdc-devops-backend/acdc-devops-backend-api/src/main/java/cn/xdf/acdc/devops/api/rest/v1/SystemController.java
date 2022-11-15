package cn.xdf.acdc.devops.api.rest.v1;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/v1/system")

public class SystemController {

    private static final String HEALTH_CHECK_RESULT = "ok";

    /**
     * Health check.
     *
     * @return ok
     */
    @GetMapping("/health-check")
    public String healthCheck() {
        return HEALTH_CHECK_RESULT;
    }
}
