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
        .chart-border{
            border: 1px solid #b0c0bf;
            border-radius:10px;
        }
    </style>

</head>

<body id="main">
<div id="left" style="width: 28%; height: 100%; float: left; margin-top: 2px;">
    <div id="left1" style="height: 32%;" class="chart-border"></div>
    <div id="left2" style="height: 33%;" class="chart-border"></div>
    <div id="left3" style="height: 33%;" class="chart-border"></div>
</div>
<div id="mid" style="width: 44%; height: 100%; float: left; margin-top: 2px;">
    <div>
        <div style="text-align: center; font-size: 20px; color: #dbfffa; line-height: 43px">XX商家流量分析系统</div>
        <div id="link" style="float:right; height: 40px" ><img src="images/arrow.jpg"></div>
    </div>
    <div id="mid1" style="height: 35%;" class="chart-border"></div>
    <div id="mid2" style="height: 59%;" class="chart-border"></div>
</div>
<div id="right" style="width: 28%; height: 100%; float: right; margin-top: 2px;">
    <div id="right1" style="height: 32%;" class="chart-border"></div>
    <div id="right2" style="height: 33%;" class="chart-border"></div>
    <div id="right3" style="height: 33%;" class="chart-border"></div>
</div>

<!-- 中间区域 ：实时流式分析 -->
<script type="text/javascript">
    var mid1Chart = echarts.init(document.getElementById('mid1'));
    var mid2Chart = echarts.init(document.getElementById('mid2'));
    var left1Chart = echarts.init(document.getElementById('left1'));
    var left2Chart = echarts.init(document.getElementById('left2'));
    var left3Chart = echarts.init(document.getElementById('left3'));
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
                x: 'left',
                text: '商家实时交易次数(TOP10)',
                subtext: '',
                textStyle: {
                    color: '#ffffff',
                    fontSize: 14,
                    fontWeight: 'lighter'
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
                top: 40,
                left: 5,
                right: 15,
                bottom: 5,
                containLabel: true
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
                        }
                    }
                }
            ]
        };

        left1Chart.setOption(option_head);
    }

    shopRankList();
    //定时刷新 定位毫秒
    setInterval(shopRankList, interval * 1000);


    function province_ajaxQuery() {
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
                        color: '#ffffff',
                        fontSize: 14,
                        fontWeight: 'lighter'
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
                        zoom: 1.2,
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

        mid2Chart.setOption(map_option, true);

    }

    province_ajaxQuery();
    //定时任务 刷新地图
    setInterval(province_ajaxQuery, interval * 1000);

    mid2Chart.on('click', function (params) {
        var seriesType = params.seriesType;
        if (seriesType == 'bar') {
            var url = params.data.url;
            window.open(url);
        }
    });

    <!-- 左侧区域 ：presto查询 -->
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
                pay.push(item.payTimes*1.0/1000);
                view.push(item.viewTime*1.0/1000);
            })
            var types = ["交易次数","浏览次数"]
            var datas = [pay,view];
            var option = {
                title: {
                    text: "口碑交易统计(K)",
                    x: 'center',
                    textStyle: {
                        color: '#ffffff',
                        fontSize: 14,
                        fontWeight: 'lighter'
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
                    left: 5,
                    right: 15,
                    bottom: 10,
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
            mid1Chart.setOption(option);
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
                        color: '#ffffff',
                        fontSize: 14,
                        fontWeight: 'lighter'
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
                        name: '消费金额',
                        type: 'pie',
                        radius : [40, 120],
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
            left2Chart.setOption(option);
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
                        color: '#ffffff',
                        fontSize: 14,
                        fontWeight: 'lighter'
                    },

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
                    left: 50,
                    right: 40,
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
                    axisLabel: {inside: true,interval: 0, textStyle: {fontSize: 13,color: '#ffffff'}},
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
                                        '#FE8463','#9BCA63','#ADCFFF','#86fa9d','#F3A43B','#60C0DD'
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
            left3Chart.setOption(option);
        });
    }

    ajaxQuery();
</script>
<!-- 右侧区域：Spark SQL、Spark Dataframe -->
<script>
    var right1Chart = echarts.init(document.getElementById('right1'));
    var right2Chart = echarts.init(document.getElementById('right2'));
    var right3Chart = echarts.init(document.getElementById('right3'));

    function shop_ajaxQuery() {
        /**
         * 最受欢迎奶茶排行
         */
        var shops_milktea = [];
        var shops_fastfood = [];
        $.get({url: "common/query_getPopulShop?cate=奶茶"}).done(function (data1) {
            data1.map(function (item1) {
                shops_milktea.push({name:item1.shopId, value: item1.grade});
            });
        });
        $.get({url: "common/query_getPopulShop?cate=中式快餐"}).done(function (data2) {
            data2.map(function (item2) {
                shops_fastfood.push({name:item2.shopId, value: item2.grade});
            });
            right2Chart.setOption(option);
        });

        var option = {
            title: {
                x: 'left',
                text: '最受欢迎商店',
                subtext: '',
                textStyle: {
                    color: '#ffffff',
                    fontSize: 14,
                    fontWeight: 'lighter'
                }
            },
            tooltip : {
                trigger: 'item',
                formatter: "{a} <br/>{b} : {c} ({d}%)"
            },
            legend: {
                orient : 'vertical',
                x : 'left',
                data:['直达','营销广告','搜索引擎','邮件营销','联盟广告','视频广告','百度','谷歌','必应','其他']
            },
            toolbox: {
                show : false,
                feature : {
                    mark : {show: true},
                    dataView : {show: true, readOnly: false},
                    magicType : {
                        show: true,
                        type: ['pie', 'funnel']
                    },
                    restore : {show: true},
                    saveAsImage : {show: true}
                }
            },
            calculable : false,
            series : [
                {
                    name:'奶茶',
                    type:'pie',
                    selectedMode: 'single',
                    radius : [0, 55],

                    // for funnel
                    x: '20%',
                    width: '40%',
                    funnelAlign: 'right',
                    max: 1548,

                    itemStyle : {
                        normal : {
                            label : {
                                position : 'inner'
                            },
                            labelLine : {
                                show : false
                            }
                        }
                    },
                    data: shops_milktea
                },
                {
                    name:'中式快餐',
                    type:'pie',
                    radius : [80, 110],

                    // for funnel
                    x: '60%',
                    width: '35%',
                    funnelAlign: 'left',
                    max: 1048,

                    data: shops_fastfood
                }
            ]
        };

    }
    shop_ajaxQuery();

    function trade_ajaxQuery() {
        /**
         * 平均日交易额最大商家排行
         */
        var shops = [];
        $.get({url: "common/query_getTradeAccount"}).done(function (data) {

            data.map(function (item) {
                shops.push({value: item.tradeCount, name: item.shopId});
            });
            right1Chart.setOption(option);
        });
        var option = {
            title : {
                text: '平均日交易额最大商家',
                textStyle: {
                    color: '#ffffff',
                    fontSize: 14,
                    fontWeight: 'lighter'
                }
            },
            tooltip : { //弹窗提示
                trigger: 'item',
                formatter: "商家{b} : 交易额{c}"
            },
            grid: {
                top: 20,
                left: 5,
                right: 15,
                bottom: 10,
                containLabel: true
            },
            color: ['#ffa377', '#FFFF00', '#33ff00', '#33ffff', '#6DFFA7'],
            toolbox: {
                show : false,
                feature : {
                    mark : {show: true},
                    dataView : {show: true, readOnly: false},
                    restore : {show: true},
                    saveAsImage : {show: true}
                }
            },
            calculable : true,
            series : [
                {
                    name:'交易额',
                    type:'funnel',
                    width: '80%',
                    left: '10%',
                    // height: {totalHeight} - y - y2,
                    height: '80%',
                    sort : 'descending', // 'ascending', 'descending'
                    gap : 8,
                    itemStyle: {
                        normal: {
                            // color: 各异,
                            //borderColor: '#fff',f
                            opacity: 0.7,
                            borderWidth: 1,
                            label: {
                                show: true,
                                position: 'inside'
                                // textStyle: null      // 默认使用全局文本样式，详见TEXTSTYLE
                            },
                            labelLine: {
                                show: false,
                                length: 10,
                                lineStyle: {
                                    // color: 各异,
                                    width: 1,
                                    type: 'solid'
                                }
                            }
                        },
                        emphasis: {
                            // color: 各异,
                            //borderColor: 'red',
                            borderWidth: 2,
                            /*label: {
                                show: true,
                                formatter: '{b}:{c}',
                                textStyle:{
                                    fontSize:14
                                }
                            },*/
                            labelLine: {
                                show: true
                            }
                        }
                    },
                    data: shops
                }
            ]
        };
    }
    trade_ajaxQuery();

    /**
     * 浏览次数最多的50个商家，并输出他们的城市以及人均消费
     */
    function view_ajaxQuery() {
        $.get({url: "common/query_getMostViewShop"}).done(function (data) {
            var shops = [];
            var pays = [];
            var views = [];
            var types = ["浏览次数","平均消费"]
            data.map(function (item) {
                shops.push(item.shopId);
                pays.push(item.perPay);
                views.push(item.viewTimes/1000);
            });
            var option = {
                title: {
                    text: "商家浏览量(K)",
                    x: 'left',
                    textStyle: {
                        color: '#ffffff',
                        fontSize: 14,
                        fontWeight: 'lighter'
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
                    left: 5,
                    right: 15,
                    bottom: 10,
                    containLabel: true
                },
                xAxis : [
                    {
                        type : 'category',
                        boundaryGap : false,
                        splitLine: {show: false},
                        axisLine: {lineStyle: {color: '#ffffff'}},
                        data: shops
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
                                color: '#55deb9',
                                lineStyle: {
                                    width: 3,
                                    type: 'dashed'
                                }
                            }
                        },
                        data: views
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
                                color: '#eee761',
                                lineStyle:{
                                    width:3,
                                    type:'dashed'  //'dotted'虚线 'solid'实线
                                }
                            }
                        },
                        data: pays
                    }
                ]
            };
            right3Chart .setOption(option);
        });

    }
    view_ajaxQuery()
</script>
</body>
</html>