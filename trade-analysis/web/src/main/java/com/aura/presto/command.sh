#!/usr/bin/env bash

#在Hive中创建user_pay和user_view表
#先创建外部表：
create external table if not exists user_pay_external (
user_id INT,
shop_id INT,
pay_time TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/trade-analysis/user_pay/';

create external table if not exists user_view_external (
user_id INT,
shop_id INT,
view_time TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/trade-analysis/user_view/';
#再创建orc表：
create table if not exists user_pay_orc (
user_id INT,
shop_id INT,
pay_time TIMESTAMP
)
stored as orc;

create table if not exists user_view_orc (
user_id INT,
shop_id INT,
view_time TIMESTAMP
)
stored as orc;
#导入数据：
insert into table user_view_orc select * from user_view_external;
insert into table user_pay_orc select * from user_pay_external;