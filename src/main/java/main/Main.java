package main;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;

public class Main {
    private static final String DEFAULT_CONFIG_LOCATION = "config.properties";

    private static Configurations configs;
    private static String location = "";

    public static void main(String[] args) {
        setGlobalConfig(DEFAULT_CONFIG_LOCATION);
    }

    public static void setGlobalConfig(String s) {
        if(configs == null) {
            configs = new Configurations();
        }
        location = s;
    }

    public static Configuration getGlobalConfig() {
        if(configs == null) {
            System.out.println("Configurations not set! Call setGlobalConfig first");
            return null;
        }
        try {
            return configs.properties(new File(location));
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        return null;
    }
}
