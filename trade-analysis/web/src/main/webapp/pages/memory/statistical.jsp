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
            height: 584px;
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
<div id="chartContent1" style="width: 33%;float: right;margin-top:10px;" class="chart marginRight"></div>
<div id="chartContent2" style="width: 33%;float: right;margin-top:10px;" class="chart marginRight"></div>

<script type="text/javascript">
    var height = $(window).height() / 2 - 15;
    $("#chartContent").height(height);

    var chartContent1 = echarts.init(document.getElementById('chartContent1'));
    var chartContent2 = echarts.init(document.getElementById('chartContent2'));

    function ajaxQuery() {

        /**
         * 最受欢迎排行
         */
        $.get({url: "common/query_getPopulShop?cate=奶茶"}).done(function (data) {
            data = data.sort(function (a, b) {
                return a.uv - b.uv;
            })
            var titles = [];
            var uvs = [];
            data.map(function (item) {
                titles.push(subTitle(item.title, 22) + " : " + item.uv);
                uvs.push(item.uv);
            })
            var option = getOptionContent("最受欢迎奶茶商店排行", titles, uvs);
            chartContent1.setOption(option);
        });

        /**
         * 最受欢迎排行
         */
        $.get({url: "common/query_getPopulShop?cate=中式快餐"}).done(function (data) {
            data = data.sort(function (a, b) {
                return a.uv - b.uv;
            })
            var titles = [];
            var uvs = [];
            data.map(function (item) {
                titles.push(subTitle(item.title, 22) + " : " + item.uv);
                uvs.push(item.uv);
            })
            var option = getOptionContent("最受欢迎中式快餐排行", titles, uvs);
            chartContent2.setOption(option);
        });
    }

    function subTitle(title, length) {
        if (title.length > length) {
            return title.substring(0, length) + "...";
        }
        return title;
    }

    ajaxQuery();
</script>
</body>
</html>