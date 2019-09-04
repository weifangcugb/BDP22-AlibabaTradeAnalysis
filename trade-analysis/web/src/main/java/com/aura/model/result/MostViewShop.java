package com.aura.model.result;

public class MostViewShop {
    private Long shopId;
    private String cityName;
    private Long perPay;
    private Long viewTimes;

    public Long getViewTimes() {
        return viewTimes;
    }

    public void setViewTimes(Long viewTimes) {
        this.viewTimes = viewTimes;
    }

    public Long getShopId() {
        return shopId;
    }

    public void setShopId(Long shopId) {
        this.shopId = shopId;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public Long getPerPay() {
        return perPay;
    }

    public void setPerPay(Long perPay) {
        this.perPay = perPay;
    }
}
