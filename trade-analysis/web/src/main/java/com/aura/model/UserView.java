package com.aura.model;

import java.io.Serializable;

/**
 * 功能描述
 * 用户浏览行为
 * @author duanshihui
 * @date 2019-08-02
 */
public class UserView implements Serializable {

    /**
     * 用户id
     */
    private String user_id;
    /**
     * 商家id
     */
    private String shop_id;
    /**
     * 浏览时间，格式：2015-10-10 10:10:10
     */
    private String time_stamp;

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getShop_id() {
        return shop_id;
    }

    public void setShop_id(String shop_id) {
        this.shop_id = shop_id;
    }

    public String getTime_stamp() {
        return time_stamp;
    }

    public void setTime_stamp(String time_stamp) {
        this.time_stamp = time_stamp;
    }
}
