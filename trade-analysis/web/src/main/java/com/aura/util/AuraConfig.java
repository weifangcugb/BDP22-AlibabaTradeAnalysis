package com.aura.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

public class AuraConfig {

    private static Config root = loadConfig();

    private static Config loadConfig() {
        String confPath = System.getenv("AURA_CONF");
        if (confPath != null) {
            return ConfigFactory.parseFile(new File(confPath));
        } else {
            return ConfigFactory.parseResources("aura.conf");
        }
    }

    public static Config getRoot() {
        return root;
    }

    public static Config getPrestoConfig() {
        return root.getConfig("presto");
    }

    public static Config getStreamingConfig() {
        return root.getConfig("streaming");
    }

    public static Config getKafkaConfig() {return root.getConfig("kafka");};

}
