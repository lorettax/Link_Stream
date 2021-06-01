package com.lorettax.StreamExtractionEngine.core;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinkStreamFQLConfig {

    private static final Logger logger = LoggerFactory.getLogger(LinkStreamFQLConfig.class);

    private static volatile JSONObject CONFIG;

    public static JSONObject getConfig() {
        if (CONFIG != null) {
            return CONFIG;
        }

        synchronized (LinkStreamFQLConfig.class) {
            if (CONFIG != null) {
                return CONFIG;
            }
            CONFIG = new JSONObject();
            return CONFIG;
        }
    }

    public static JSONObject duplicateConfig() {
        return copyConfig(getConfig());
    }

    private static JSONObject copyConfig(JSONObject config) {
        return JSONObject.parseObject(JSONObject.toJSONString(config));
    }

    public static void loadConfig(JSONObject config) {
        synchronized (LinkStreamFQLConfig.class) {
            CONFIG = copyConfig(config);
        }
    }


}


