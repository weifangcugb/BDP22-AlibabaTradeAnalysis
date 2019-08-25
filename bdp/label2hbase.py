# -*- coding: utf-8 -*-
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime


class LabelData(object):

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read("config.ini", encoding="utf-8")
        self.last_7_days_date = datetime.strptime("2016-10-24", '%Y-%m-%d').date()
        self.last_1_month_date = datetime.strptime("2016-09-30", '%Y-%m-%d').date()
        self.last_3_month_date = datetime.strptime("2016-07-31", '%Y-%m-%d').date()

    def user_view_count(self, date_before, check_type):
        """
        用户相关统计
        :return:
        """
        data_path = self.config.get('DEV.HDFS', 'PATH')
        data_type = "user_view.txt" if check_type == "user_view" else "user_pay.txt"
        master = self.config.get('DEV.HDFS', 'MASTER')
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("shop_id", StringType()),
            StructField("time_stamp", DateType())
        ])
        app_name = f"user|| {date_before}||{check_type}"
        print(app_name)
        builder = SparkSession.builder \
            .master(master) \
            .appName(app_name) \
            .getOrCreate()
        data_frame = builder.read.csv(data_path + data_type, schema)
        filter_sql = f"time_stamp > '{date_before}'"
        dd = data_frame.filter(filter_sql).groupBy('user_id').count()
        dd.show()


    def shop_view_count(self, date_before, check_type):
        """
        店铺相关统计
        :return:
        """
        data_path = self.config.get('DEV.HDFS', 'PATH')
        data_type = "user_view.txt" if check_type == "user_view" else "user_pay.txt"
        master = self.config.get('DEV.HDFS', 'MASTER')
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("shop_id", StringType()),
            StructField("time_stamp", DateType())
        ])
        app_name = f"shop|| {date_before} ||{check_type}"
        print(app_name)
        builder = SparkSession.builder \
            .master(master) \
            .appName(app_name) \
            .getOrCreate()
        data_frame = builder.read.csv(data_path + data_type, schema)
        filter_sql = f"time_stamp > '{date_before}'"
        dd = data_frame.filter(filter_sql).groupBy('shop_id').count()
        dd.show()

    def schedule_check(self):
        ##  调度
        # 用户标签 浏览店铺
        self.user_view_count(self.last_7_days_date, check_type="user_view")
        self.user_view_count(self.last_1_month_date, check_type="user_view")
        self.user_view_count(self.last_3_month_date, check_type="user_view")
        # 用户标签 支付
        self.user_view_count(self.last_7_days_date, check_type="user_pay")
        self.user_view_count(self.last_1_month_date, check_type="user_pay")
        self.user_view_count(self.last_3_month_date, check_type="user_pay")
        # 店铺标签 浏览用户
        self.shop_view_count(self.last_7_days_date, check_type="user_view")
        self.shop_view_count(self.last_1_month_date, check_type="user_view")
        self.shop_view_count(self.last_3_month_date, check_type="user_view")
        # 店铺标签 支付
        self.shop_view_count(self.last_7_days_date, check_type="user_pay")
        self.shop_view_count(self.last_1_month_date, check_type="user_pay")
        self.shop_view_count(self.last_3_month_date, check_type="user_pay")

if __name__ == '__main__':
    data = LabelData()
    data.schedule_check()
