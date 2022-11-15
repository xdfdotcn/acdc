package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.config.UIConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("api/v1/ui")
public class UiController {

    @Autowired
    private UIConfig uiConfig;

    /**
     * Get UI config.
     *
     * @return config map
     */
    @GetMapping("/config")
    public Map<String, String> getConfig() {
        return uiConfig.getConfig();
    }
}
