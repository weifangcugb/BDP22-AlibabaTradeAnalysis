package com.aura.util;

import org.apache.spark.sql.types.StructType;

public class StructTypeUtil {
    public static final StructType userPaySchema = new StructType()
            .add("user_id", "long", false)
            .add("shop_id", "long", true)
            .add("pay_time", "string", true);

    public static final StructType userViewSchema = new StructType()
            .add("user_id", "long", false)
            .add("shop_id", "long", true)
            .add("view_time", "string", true);

    public static final StructType shopInfoSchema = new StructType()
            .add("shop_id", "long", false)
            .add("city_name", "string", true)
            .add("location_id", "int", true)
            .add("per_pay", "int", true)
            .add("score", "int", true)
            .add("comment_cnt", "int", true)
            .add("shop_level", "int", true)
            .add("cate_1_name", "string", true)
            .add("cate_2_name", "string", true)
            .add("cate_3_name", "string", true);
}
