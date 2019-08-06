<%@ page language="java" import="java.util.*" pageEncoding="UTF-8" %>
<%
    String path = request.getContextPath();
    String basePath = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort() + path + "/";
%>

<!doctype html>
<html>
<head>
    <base href="<%=basePath%>">

    <title>任务四-Spark RDD和Spark Dataframe</title>

    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="description" content="">
    <meta name="keywords" content="">
    <meta name="viewport" content="width=device-width, initial-scale=1">

    <script type="text/javascript" src="pages/jquery2/jquery-2.2.3.min.js"></script>
    <script type="text/javascript" src="pages/echarts-3.2.1/echarts.min.js"></script>
    <script type="text/javascript" src="pages/echarts-3.2.1/china.js"></script>
    <script type="text/javascript" src="pages/echarts-3.2.1/world.js"></script>
    <script type="text/javascript" src="pages/js/echarts3-basic.js?v=1.1"></script>
    <script type="text/javascript" src="pages/js/memory.js?v=1.2"></script>

    <style type="text/css">
        #main {
            width: 100%;
            height: 100%;
        }

        body {
            margin: 0 0 0 0;
            background-image: url('pages/images/6.jpg');
            background-attachment: fixed;
            background-repeat: no-repeat;
            background-size: cover;
            -moz-background-size: cover;
            -webkit-background-size: cover;
        }

        .chart {

        }

        .marginRight {
            margin-right: 1px;
        }
    </style>

</head>

<body>
<div id="chartContent0" style="width: 30%;float: left;margin-top:100px;margin-left: 100px" class="chart"></div>
<div id="chartContent1" style="width: 30%;float: left;margin-top:100px;" class="chart"></div>
<div id="chartContent2" style="width: 30%;float: left;margin-top:100px;" class="chart marginRight"></div>
<script type="text/javascript">
    var height = $(window).height() / 2 - 15;
    $("#chartContent0").height(height);
    $("#chartContent1").height(height);
    $("#chartContent2").height(height);

    var chartContent0 = echarts.init(document.getElementById('chartContent0'));
    var chartContent1 = echarts.init(document.getElementById('chartContent1'));
    var chartContent2 = echarts.init(document.getElementById('chartContent2'));

    function ajaxQuery() {

        /**
         * 平均日交易额最大商家排行
         */
        $.get({url: "common/query_getTradeAccount"}).done(function (data) {
            var shops = [];
            var trades = [];
            data.map(function (item) {
                shops.push(item.shopId + " : " + item.tradeCount);
                trades.push(item.tradeCount);
            });
            var option = getOptionContent0("平均日交易额最大商家排行", shops, trades);
            chartContent0.setOption(option);
        });

        /**
         * 最受欢迎奶茶排行
         */
        $.get({url: "common/query_getPopulShop?cate=奶茶"}).done(function (data) {
            var shops = [];
            var grades = [];
            data.map(function (item) {
                shops.push(item.shopId + " : " + item.grade);
                grades.push(item.grade);
            });
            var option = getOptionContent1("最受欢迎奶茶商店排行", shops, grades);
            chartContent1.setOption(option);
        });

        /**
         * 最受欢迎中式排行
         */
        $.get({url: "common/query_getPopulShop?cate=中式快餐"}).done(function (data) {
            var shops = [];
            var grades = [];
            data.map(function (item) {
                shops.push(item.shopId + " : " + item.grade);
                grades.push(item.grade);
            });
            var option = getOptionContent2("最受欢迎中式快餐排行", shops, grades);
            chartContent2.setOption(option);
        });
    }
    ajaxQuery();
</script>
</body>
</html>