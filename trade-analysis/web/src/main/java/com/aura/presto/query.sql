Hive: presto --server master:8081 --catalog hive --schema default
MySQL: presto --server master:8081 --catalog mysql --schema aura

1.以城市为单位，统计每个城市总体消费金额 （饼状图）：
select a.city_name,sum(a.per_pay) from hive.default.user_pay_orc b join mysql.aura.shop_info a on a.shop_id = b.shop_id groupby a.city_name;

2.以天为单位，统计所有商家交易发生次数和被用户浏览次数 （曲线图）:
select pay.date,pay.pay_times,view.view_times from
(select substr(cast(a.pay_time as varchar),1,10) as date,count(*) as pay_times  from user_pay_orc a group by substr(cast(a.pay_time as varchar),1,10)) pay
join
(select substr(cast(b.view_time as varchar),1,10) as date,count(*) as view_times  from user_view_orc b group by substr(cast(b.view_time as varchar),1,10)) view
on pay.date = view.date order by pay.date desc;

3.统计最受欢迎的前10类商品（按照二级分类统计），并输出他们的人均消费（选择合适图表对其可视化，类似排行榜）：
select b.cate_2_name,cast(sum(b.per_pay)*1.0/count(*) as decimal(10,2)) from user_pay_orc a join mysql.aura.shop_info b on a.shop_id = b.shop_id
group by b.cate_2_name order by cast(sum(b.per_pay)*1.0/count(*) as decimal(10,2)) desc limit 10;