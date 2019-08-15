<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%
    String path = request.getContextPath();
    String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<!doctype html>
<html>
<head>
    <base href="<%=basePath%>">

    <title>内存计算框架-Spark Core</title>

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
            background-image: url('pages/images/4.jpg');
            background-attachment:fixed;
            background-repeat:no-repeat;
            background-size:cover;
            -moz-background-size:cover;
            -webkit-background-size:cover;
        }
        .chart {

        }
        .marginRight {
            margin-right: 1px;
        }
    </style>

</head>

<body>
<div id="chartFlow" style="width: 80%;float: left;margin-top:15px; margin-left: 10%" class="chart"></div>
<div id="chartSearch" style="width: 33%;float: left;margin-top:10px; margin-left: 15%" class="chart"></div>
<div id="chartContent" style="width: 33%;float: right;margin-top:10px; margin-right: 10%" class="chart marginRight"></div>

<script type="text/javascript">
    var height = $(window).height() / 2 - 15;
    $("#chartFlow").height(height);
    $("#chartSearch").height(height);
    $("#chartContent").height(height);

    var chartFlow = echarts.init(document.getElementById('chartFlow'));
    var chartSearch = echarts.init(document.getElementById('chartSearch'));
    var chartContent = echarts.init(document.getElementById('chartContent'));

    function ajaxQuery() {
        /**
         * 以天为单位，统计所有商家交易发生次数和被用户浏览次数（曲线图）
         */
        $.get({url:"common/query_getShopTradeView"}).done(function(data) {
            var days = [];
            var pay = [];
            var view = [];
            data.map(function(item) {
                days.push(item.date);
                pay.push(item.payTimes);
                view.push(item.viewTime);
            })
            var types = ["商家交易次数","商家浏览次数"]
            var datas = [pay,view];
            var option = {
                title: {
                    text: "口碑交易统计（以天为单位）",
                    textStyle: {
                        color: '#ffffff'
                    }
                },
                tooltip : {
                    trigger: 'axis'
                },
                legend: {
                    textStyle: {
                        color: '#ffffff'
                    },
                    x : 'right',
                    y : 'top',
                    data: types
                },
                grid: {
                    top: 40,
                    left: 30,
                    right: 32,
                    bottom: 5,
                    containLabel: true
                },
                xAxis : [
                    {
                        type : 'category',
                        boundaryGap : false,
                        splitLine: {show: false},
                        axisLine: {lineStyle: {color: '#ffffff'}},
                        data: days
                    }
                ],
                yAxis : [
                    {
                        type : 'value',
                        splitLine: {show: false},
                        axisLine: {lineStyle: {color: '#ffffff'}}
                    }
                ],
                series : [
                    {
                        name:types[0],
                        type:'line',
                        symbol: 'emptyTriangle',
                        symbolSize: 10,
                        markPoint: {
                            data: [
                                {type: 'max', name: '最大值'},
                                {type: 'min', name: '最小值'}
                            ]
                        },
                        itemStyle: {
                            normal: {
                                color: '#de4c4f'/*,
                                lineStyle: {
                                    width: 2,
                                    type: 'dashed'
                                }*/
                            }
                        },
                        data:datas[0]
                    },
                    {
                        name:types[1],
                        type:'line',
                        symbol: 'circle',
                        symbolSize: 10,
                        smooth: true,
                        markPoint: {
                            data: [
                                {type: 'max', name: '最大值'},
                                {type: 'min', name: '最小值'}
                            ]
                        },
                        itemStyle: {
                            normal: {
                                color: '#eea638'
                            }
                        },
                        data:datas[1]
                    }
                ]
            };
            chartFlow.setOption(option);
        });

        /**
         * 以城市为单位，统计每个城市总体消费金额 （饼状图）
         */
        $.get({url:"common/query_getCityConsume"}).done(function(data) {
            data.map(function(item) {
                item.name= item.cityName;
                item.value = item.consume;
            });
            var option = {
                title : {
                    text: "城市总体消费金额（TOP10）",
                    x:'left',
                    textStyle: {
                        color: '#ffffff'
                    }
                },
                tooltip : {
                    trigger: 'item'
                },
                legend: {
                    x : 'right',
                    y : 'top',
                    textStyle: {
                        color: '#ffffff'
                    },
                    data:[]
                },
                calculable : true,
                series : [
                    {
                        name: '搜索引擎',
                        type: 'pie',
                        radius : [40, 160],
                        center : ['53%', '60%'],
                        roseType : 'area',
                        label: {
                            normal: {
                                show: true,
                                textStyle: {
                                    color: '#ffffff'
                                }
                            }
                        },
                        color: ['#d87a80','#ffb980','#008acd','#b6a2de','#2ec7c9'],
                        data: data
                    }
                ]
            };
            chartSearch.setOption(option);
        });

        /**
         * 统计最受欢迎的前10类商品（按照二级分类统计），并输出他们的人均消费（选择合适图表对其可视化，类似排行榜）
         */
        $.get({url:"common/query_getPopuShopTrade"}).done(function(data) {
            data = data.sort(function (a, b) {
                return a.perPay - b.perPay;
            })
            var shops = [];
            var pays = [];
            var titles =[];
            data.map(function(item) {
                shops.push(item.shop);
                pays.push(item.perPay);
                titles.push(item.shop +":"+item.perPay);
            })
            var option = {
                title: {
                    text: "最受欢迎商品（二级分类）",
                    textStyle: {
                        color: '#ffffff'
                    }
                },
                tooltip: {
                    trigger: 'axis',
                    axisPointer: {
                        type: 'shadow'
                    }
                },
                legend: {
                    textStyle: {
                        color: '#ffffff'
                    },
                    data: []
                },
                grid: {
                    top: 30,
                    left: 5,
                    right: 20,
                    bottom: 5,
                    containLabel: true
                },
                xAxis: {
                    type: 'value',
                    splitLine: {show: false},
                    axisLine: {lineStyle: {color: '#ffffff'}},
                    boundaryGap: [0, 0.01]
                },
                yAxis: {
                    type: 'category',
                    splitLine: {show: false},
                    axisLine: {show: false, lineStyle: {color: '#ddd'}},
                    axisTick: {show: false, lineStyle: {color: '#ddd'}},
                    axisLabel: {inside: true,interval: 0, textStyle: {fontSize: 15,color: '#ffffff'}},
                    zlevel: 3,
                    data: titles
                },
                series: [
                    {
                        name: '人均消费',
                        type: 'bar',
                        itemStyle: {
                            normal: {
                                color: function(params) {
                                    var colorList = [
                                        '#C1232B','#B5C334','#FCCE10','#E87C25','#27727B',
                                        '#FE8463','#9BCA63','#FAD860','#F3A43B','#60C0DD',
                                        '#D7504B','#C6E579','#F4E001','#F0805A','#26C0C0'
                                    ];
                                    if(params.dataIndex < 0) {
                                        return colorList[0];
                                    }
                                    var index = params.dataIndex % colorList.length;
                                    var dataSize = shops.length;
                                    if(dataSize > colorList.length) {
                                        dataSize = colorList.length;
                                    }
                                    index = dataSize - index - 1;
                                    return colorList[index];
                                }
                            }
                        },
                        data: pays
                    }
                ]
            };
            chartContent.setOption(option);
        });
    }

    ajaxQuery();
</script>
</body>
</html>