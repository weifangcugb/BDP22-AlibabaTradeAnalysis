<%@ page language="java" import="java.util.*" pageEncoding="UTF-8" %>
<%
    String path = request.getContextPath();
    String basePath = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort() + path + "/";
%>

<!doctype html>
<html>
<head>
    <base href="<%=basePath%>">

    <title>商家流量分析系统</title>

    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="description" content="">
    <meta name="keywords" content="">
    <meta name="viewport" content="width=device-width, initial-scale=1">

    <script type="text/javascript" src="pages/jquery2/jquery-2.2.3.min.js"></script>
    <script type="text/javascript" src="pages/echarts-3.2.1/echarts.min.js"></script>
    <script type="text/javascript" src="pages/echarts-3.2.1/china.js"></script>
    <script type="text/javascript" src="pages/echarts-3.2.1/world.js"></script>

    <style type="text/css">
        body {
            height: 100%;
            margin: 0 0 0 0;
            background-image: url('pages/images/4.jpg');
            background-attachment: fixed;
            background-repeat: no-repeat;
            background-size: cover;
            -moz-background-size: cover;
            -webkit-background-size: cover;
        }
        html,body {
            width: 100%;
            height: 100%;
        }
    </style>

</head>

<body>
<div id="left" style="width: 26%; height: 100%; float: left; margin-top: 10px; border: 1px solid #c0a16b">
    <div id="left1" style="height: 33%;border: 1px solid #73a6c0"></div>
    <div id="left2" style="height: 33%;border: 1px solid #73a6c0"></div>
    <div id="left3" style="height: 33%;border: 1px solid #73a6c0"></div>
</div>
<div id="mid" style="width: 44%; height: 100%; float: left; margin-left: 2%; margin-top: 10px; border: 1px solid #c0a16b">
    <div style="text-align: center; font-size: 18px; color: #ffffff; line-height: 40px">商家流量分析系统</div>
    <div id="mid1" style="height: 35%;border: 1px solid #73a6c0"></div>
    <div id="mid2" style="height: 60%;border: 1px solid #73a6c0"></div>
</div>
<div id="right" style="width: 26%; height: 100%; float: right; margin-top: 10px; border: 1px solid #c0a16b">
    <div id="right1" style="height: 33%;border: 1px solid #73a6c0"></div>
    <div id="right2" style="height: 33%;border: 1px solid #73a6c0"></div>
    <div id="right3" style="height: 33%;border: 1px solid #73a6c0"></div>
</div>

<script type="text/javascript">
    var mid1Chart = echarts.init(document.getElementById('mid1'));
    var mid2Chart = echarts.init(document.getElementById('mid2'));
    var interval = 10;

    //fetchStreamingStartTime();
    function shopRankList() {

        var shop = [];
        var trade = [];

        var getUrl = "common/query_getMerchantTrade";
        $.ajax({
            async: false,
            url: getUrl,
            type: "get",
            dataType: "json",
            success: function (data) {
                data.map(function (item) {
                    shop.push(item.shopId);
                    trade.push(item.tradeCount);
                });
            }
        });

        var option_head = {
            title: {
                x: 'center',
                text: '商家实时交易次数(TOP10)',
                subtext: '',
                textStyle: {
                    color: '#3B6C88'
                }
            },
            tooltip: {
                trigger: 'item'
            },
            toolbox: {
                show: false,
                feature: {
                    dataView: {show: true, readOnly: false},
                    restore: {show: true},
                    saveAsImage: {show: true}
                }
            },
            calculable: true,
            grid: {
                borderWidth: 0,
                y: 80,
                y2: 60
            },
            xAxis: [
                {
                    type: 'category',
                    show: false,
                    data: shop
                }
            ],
            yAxis: [
                {
                    type: 'value',
                    show: false
                }
            ],
            series: [
                {
                    name: '交易量',
                    type: 'bar',
                    itemStyle: {
                        normal: {
                            color: function (params) {
                                // build a color map as your need.
                                var colorList = [
                                    '#C1232B', '#B5C334', '#FCCE10', '#E87C25', '#27727B',
                                    '#FE8463', '#9BCA63', '#FAD860', '#F3A43B', '#60C0DD',
                                    '#D7504B', '#C6E579', '#F4E001', '#F0805A', '#26C0C0'
                                ];
                                return colorList[params.dataIndex]
                            },
                            label: {
                                show: true,
                                position: 'top',
                                formatter: '{b}\n{c}'
                            }
                        }
                    },
                    data: trade,
                    markPoint: {
                        tooltip: {
                            trigger: 'item',
                            backgroundColor: 'rgba(0,0,0,0)',
                            formatter: function (params) {
                                return '<img src="'
                                    + params.data.symbol.replace('image://', '')
                                    + '"/>';
                            }
                        },
                        data: [
                            {xAxis: 0, y: 350, name: 'Line', symbolSize: 20, symbol: 'image://../asset/ico/折线图.png'},
                            {xAxis: 1, y: 350, name: 'Bar', symbolSize: 20, symbol: 'image://../asset/ico/柱状图.png'},
                            {xAxis: 2, y: 350, name: 'Scatter', symbolSize: 20, symbol: 'image://../asset/ico/散点图.png'},
                            {xAxis: 3, y: 350, name: 'K', symbolSize: 20, symbol: 'image://../asset/ico/K线图.png'},
                            {xAxis: 4, y: 350, name: 'Pie', symbolSize: 20, symbol: 'image://../asset/ico/饼状图.png'},
                            {xAxis: 5, y: 350, name: 'Radar', symbolSize: 20, symbol: 'image://../asset/ico/雷达图.png'},
                            {xAxis: 6, y: 350, name: 'Chord', symbolSize: 20, symbol: 'image://../asset/ico/和弦图.png'},
                            {xAxis: 7, y: 350, name: 'Force', symbolSize: 20, symbol: 'image://../asset/ico/力导向图.png'},
                            {xAxis: 8, y: 350, name: 'Map', symbolSize: 20, symbol: 'image://../asset/ico/地图.png'},
                            {xAxis: 9, y: 350, name: 'Gauge', symbolSize: 20, symbol: 'image://../asset/ico/仪表盘.png'},
                            {xAxis: 10, y: 350, name: 'Funnel', symbolSize: 20, symbol: 'image://../asset/ico/漏斗图.png'},
                        ]
                    }
                }
            ]
        };

        myChartHead.setOption(option_head);
    }

    shopRankList();
    //定时刷新 定位毫秒
    //setInterval(shopRankList, interval * 1000);


    function ajaxQuery() {
        var getUrl = "common/query_getProvinceTrade";
        var areadata = [];
        var maxValue = 0;

        $.ajax({
            async: false,
            url: getUrl,
            type: "get",
            dataType: "json",
            success: function (data) {
                data.map(function (item) {
                    if (item.tradeCount > maxValue) {
                        maxValue = item.tradeCount;
                    }
                    areadata.push({name: item.provinceName, value: item.tradeCount});
                });
            }
        });

        var map_option =
            {
                title: {
                    text: '各省份实时交易数据',
                    x: 'center',
                    textStyle: {
                        color: '#3B6C88'
                    }
                },
                tooltip: {
                    trigger: 'item'
                },
                dataRange: {
                    orient: 'horizontal',
                    min: 0,
                    max: maxValue,
                    text: ['高', '低'],           // 文本，默认为数值文本
                    splitNumber: 0,
                    inRange: {
                        color: ['#FE8463', '#9BCA63', '#FAD860', '#60C0DD']
                    }

                },
                toolbox: {
                    show: false,
                    orient: 'vertical',
                    x: 'right',
                    y: 'center',
                    feature: {
                        mark: {show: true},
                        dataView: {show: true, readOnly: false}
                    }
                },
                series: [
                    {
                        name: '成交量',
                        type: 'map',
                        mapType: 'china',
                        mapLocation: {
                            x: 'left'
                        },
                        zoom: 1.1,
                        selectedMode: 'multiple',
                        itemStyle: {
                            normal: {label: {show: true}, borderColor: "#389BB7"},
                            emphasis: {label: {show: true}}
                        },
                        data: areadata   //[{name:"北京",value:62},...]
                    }
                ],
                animation: false
            };

        myChart.setOption(map_option, true);

    }

    ajaxQuery();
    //定时任务 刷新地图
    // setInterval(ajaxQuery, interval * 1000);

    myChart.on('click', function (params) {
        var seriesType = params.seriesType;
        if (seriesType == 'bar') {
            var url = params.data.url;
            window.open(url);
        }
    });

</script>
</body>
</html>