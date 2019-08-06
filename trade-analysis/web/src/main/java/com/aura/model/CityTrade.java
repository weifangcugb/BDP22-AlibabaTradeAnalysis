package com.aura.model;

/**
 * 城市交易统计
 */
public class CityTrade {
    private String cityName;
    private Integer tradeCount;
    private String updateTime;

    public String getCityName() {
        return cityName;
    }

    public void setCityId(String cityName) {
        this.cityName = cityName;
    }

    public Integer getTradeCount() {
        return tradeCount;
    }

    public void setTradeCount(Integer tradeCount) {
        this.tradeCount = tradeCount;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
