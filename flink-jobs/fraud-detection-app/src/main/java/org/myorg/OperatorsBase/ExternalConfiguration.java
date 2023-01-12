package org.myorg.OperatorsBase;

import org.apache.flink.api.java.utils.ParameterTool;

public class ExternalConfiguration {
    private String kafkaHost;
    private int kafkaPort;
    private String kafkaInputTopic;
    private String kafkaOutputTopic;
    private String kafkaDbTopic;

    private String dbHost;
    private int dbPort;
    private String dbUser;
    private String dbPassword;
    private String dbKeyspace;

    private String redisHost;
    private int redisPort;
    private String redisPassword;
    private String redisHashKey;

    private int asyncTimeOutSec, asyncCapacity, cacheMaxSize;

    private String modelURL;
    private int cacheFlushIntervalMs;

    public ExternalConfiguration(ParameterTool parameterTool) {
        setKafkaHost(parameterTool.get("kafkaHost"));
        setKafkaPort(Integer.parseInt(parameterTool.get("kafkaPort")));
        setKafkaInputTopic(parameterTool.get("kafkaInputTopic"));
        setKafkaOutputTopic(parameterTool.get("kafkaOutputTopic"));
        setKafkaDbTopic(parameterTool.get("kafkaDbTopic"));

        setDbHost(parameterTool.get("dbHost"));
        setDbPort(Integer.parseInt(parameterTool.get("dbPort")));
        setDbUser(parameterTool.get("dbUser"));
        setDbPassword(parameterTool.get("dbPassword"));
        setDbKeyspace(parameterTool.get("dbKeyspace"));

        setAsyncTimeOutSec(parameterTool.getInt("asyncTimeoutSec", 10));
        setAsyncCapacity(parameterTool.getInt("asyncCapacity", 50));
        setCacheMaxSize(parameterTool.getInt("cacheMaxSize", 1000));

        setRedisHost(parameterTool.get("redisHost"));
        setRedisPort(parameterTool.getInt("redisPort"));
        setRedisPassword(parameterTool.get("redisPassword"));
        setRedisHashKey(parameterTool.get("redisHashKey"));

        setModelURL(parameterTool.get("modelURL"));
        setCacheFlushIntervalMs(parameterTool.getInt("cacheFlushIntervalMs", 240_000));
    }

    public String getKafkaHost() {
        return kafkaHost;
    }

    public void setKafkaHost(String kafkaHost) {
        this.kafkaHost = kafkaHost;
    }

    public int getKafkaPort() {
        return kafkaPort;
    }

    public void setKafkaPort(int kafkaPort) {
        this.kafkaPort = kafkaPort;
    }

    public String getKafkaInputTopic() {
        return kafkaInputTopic;
    }

    public void setKafkaInputTopic(String kafkaInputTopic) {
        this.kafkaInputTopic = kafkaInputTopic;
    }

    public String getKafkaOutputTopic() {
        return kafkaOutputTopic;
    }

    public void setKafkaOutputTopic(String kafkaOutputTopic) {
        this.kafkaOutputTopic = kafkaOutputTopic;
    }

    public String getDbHost() {
        return dbHost;
    }

    public void setDbHost(String dbHost) {
        this.dbHost = dbHost;
    }

    public int getDbPort() {
        return dbPort;
    }

    public void setDbPort(int dbPort) {
        this.dbPort = dbPort;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public String getDbKeyspace() {
        return dbKeyspace;
    }

    public void setDbKeyspace(String dbKeyspace) {
        this.dbKeyspace = dbKeyspace;
    }

    public void setAsyncTimeOutSec(int asyncTimeOutSec) {
        this.asyncTimeOutSec = asyncTimeOutSec;
    }

    public int getAsyncTimeOutSec() {
        return asyncTimeOutSec;
    }

    public void setAsyncCapacity(int asyncCapacity) {
        this.asyncCapacity = asyncCapacity;
    }

    public int getAsyncCapacity() {
        return asyncCapacity;
    }

    public void setCacheMaxSize(int cacheMaxSize) {
        this.cacheMaxSize = cacheMaxSize;
    }

    public int getCacheMaxSize() {
        return cacheMaxSize;
    }

    @Override
    public String toString() {
        return "ExternalConfiguration{" +
                "kafkaHost='" + kafkaHost + '\'' +
                ", kafkaPort='" + kafkaPort + '\'' +
                ", kafkaInputTopic='" + kafkaInputTopic + '\'' +
                ", kafkaOutputTopic='" + kafkaOutputTopic + '\'' +
                ", dbHost='" + dbHost + '\'' +
                ", dbPort='" + dbPort + '\'' +
                ", dbUser='" + dbUser + '\'' +
                ", dbPassword='" + dbPassword + '\'' +
                '}';
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
    }

    public String getRedisPassword() {
        return redisPassword;
    }

    public String getRedisHashKey() {
        return redisHashKey;
    }

    public void setRedisHashKey(String redisHashKey) {
        this.redisHashKey = redisHashKey;
    }

    public String getKafkaDbTopic() {
        return kafkaDbTopic;
    }

    public void setKafkaDbTopic(String kafkaDbTopic) {
        this.kafkaDbTopic = kafkaDbTopic;
    }

    public String getModelURL() {
        return modelURL;
    }

    public void setModelURL(String modelURL) {
        this.modelURL = modelURL;
    }

    public int getCacheFlushIntervalMs() {
        return cacheFlushIntervalMs;
    }

    public void setCacheFlushIntervalMs(int cacheFlushIntervalMs) {
        this.cacheFlushIntervalMs = cacheFlushIntervalMs;
    }
}
