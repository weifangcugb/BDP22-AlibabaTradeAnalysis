<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%
String path = request.getContextPath();
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<!doctype html>
<html>
  <head>
    <base href="<%=basePath%>">
    
    <title>流式计算框架-Spark Streaming</title>
    
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
	</style>

  </head>
  
  <body>
    <div id="head" style="width: 60%;float: left;margin-left: 20%;margin-top: 10px" class="chart"></div>
    <div id="main" style="width: 62%;float: left; margin-left: 27%;margin-top: -20px"></div>
    
    <script type="text/javascript">
    $("#main").height($(window).height()/3*2);
    $("#head").height($(window).height()/3);
    var myChart = echarts.init(document.getElementById('main'));

    var myChartHead = echarts.init(document.getElementById('head'));
    
    var colorCoord = ['#eea638','#de4c4f','#3398DB','#86a965','#a7a737','#8aabb0','#d8854f','#ddb926']
    
    var geoCoordMap = [
    	{name: "北京", 'color': colorCoord[0],'latitude': 39.92, 'longitude': 116.46},
    	{name: "天津", 'color': colorCoord[0],'latitude': 39.06, 'longitude': 117.28, 'position': 'right'},
    	{name: "河北", 'color': colorCoord[0],'latitude': 38.24, 'longitude': 115.29},
    	{name: "山西", 'color': colorCoord[0],'latitude': 37.08, 'longitude': 112.19},
    	{name: "内蒙古", 'color': colorCoord[0],'latitude': 41.39, 'longitude': 111.22},
    	
    	{name: "黑龙江", 'color': colorCoord[1],'latitude': 46.55, 'longitude': 128.30},
		{name: "吉林", 'color': colorCoord[1],'latitude': 43.10, 'longitude': 126.31},
		{name: "辽宁", 'color': colorCoord[1],'latitude': 41.14, 'longitude': 123.03},
		
		{name: "上海", 'color': colorCoord[2],'latitude': 31.12, 'longitude': 121.29, 'position': 'right'},
		{name: "江苏", 'color': colorCoord[2],'latitude': 33.02, 'longitude': 119.47},
		{name: "浙江", 'color': colorCoord[2],'latitude': 29.03, 'longitude': 120.12},
		{name: "安徽", 'color': colorCoord[2],'latitude': 31.37, 'longitude': 117.24},
		{name: "福建", 'color': colorCoord[2],'latitude': 26.04, 'longitude': 118.04},
		{name: "江西", 'color': colorCoord[2],'latitude': 27.29, 'longitude': 115.28},
		{name: "山东", 'color': colorCoord[2],'latitude': 36.11, 'longitude': 117.51},
		
		{name: "河南", 'color': colorCoord[3],'latitude': 33.46, 'longitude': 113.37},
    	{name: "湖北", 'color': colorCoord[3],'latitude': 30.59, 'longitude': 112.30},
		{name: "湖南", 'color': colorCoord[3],'latitude': 27.37, 'longitude': 111.42},
		
        {name: "广东", 'color': colorCoord[4],'latitude': 24.01, 'longitude': 113.19},
    	{name: "广西", 'color': colorCoord[4],'latitude': 23.40, 'longitude': 108.56},
    	{name: "海南", 'color': colorCoord[4],'latitude': 19.06, 'longitude': 109.45},
    	
		{name: "四川", 'color': colorCoord[5],'latitude': 30.32, 'longitude': 103.05},
		{name: "贵州", 'color': colorCoord[5],'latitude': 26.50, 'longitude': 106.51},
		{name: "云南", 'color': colorCoord[5],'latitude': 24.28, 'longitude': 101.56},
		{name: "西藏", 'color': colorCoord[5],'latitude': 32.10, 'longitude': 87.26},
		{name: "重庆", 'color': colorCoord[5],'latitude': 29.47, 'longitude': 107.45},
		
		{name: "陕西", 'color': colorCoord[6],'latitude': 34.07, 'longitude': 108.59},
		{name: "甘肃", 'color': colorCoord[6],'latitude': 35.02, 'longitude': 104.29},
		{name: "青海", 'color': colorCoord[6],'latitude': 35.49, 'longitude': 96.31},
		{name: "宁夏", 'color': colorCoord[6],'latitude': 37.18, 'longitude': 106.13},
		{name: "新疆", 'color': colorCoord[6],'latitude': 41.37, 'longitude': 86.36},
		
		{name: "台湾", 'color': colorCoord[7],'latitude': 23.41, 'longitude': 120.51},
		{name: "香港", 'color': colorCoord[7],'latitude': 22.25, 'longitude': 114.05, 'position': 'right'},
		{name: "澳门", 'color': colorCoord[7],'latitude': 22.12, 'longitude': 113.32, 'position': 'left'}
	];

    //根据城市名称获取位置信息
	var getCoordMapItem = function(name) {
    	var result = undefined;
    	geoCoordMap.forEach(function (item) {
    		if(name == item.name) {
    			result = item;
    		}
		});
		return result;
    }
    
    /**
     * [{title:"xxx",url:"http://xxx",value:20}] -> 
     * ["xxx:20","xxx1:30","xxx2:40"] or [{url:"http://xxx",value:20},{url:"http://xxx1",value:30},{url:"http://xxx2",value:40}]
     */
    var convertData = function (data,column) {
    	data = data.sort(function (a, b) {
	        return a.value - b.value;
	    })
	    
	    var res = [];
	    for (var i = 0; i < data.length; i++) {
	    	if(column[0] == 'title') {
	    		var title = data[i][column[0]];
	    		var value = data[i][column[1]];
	    		res.push(title + " : " + value);
	    	} else {
	    		var url = data[i][column[0]];
	    		var value = data[i][column[1]];
            	res.push({url: url, value: value});
            }
	    }
	    return res;
	};
	
	var areaColor = '#323c48';
	var borderColor = '#111';
	
	var option = {
	    // backgroundColor: '#404a59',
	    title: {
            text: '城市实时交易次数',
            subtext: '',
            left: 'center',
            top: 'top',
            textStyle: {
                color: '#3B6C88'
            }
        },
        tooltip : {
	        trigger: 'item',
	        formatter : function (params) {
	        	var type = typeof params.value;
	        	var value = params.value;
	        	if(type == 'object') {
	        		return params.name + ' : ' + value[2];
	        	}
	            return params.name;
	        }
	    },
        visualMap: {
	        show: false,
	        min: 0,
	        inRange: {
	            symbolSize: [6, 50]
	        }
	    },
	    geo: {
	        name: '地区交易量',
	        map: 'china',
	        left: 0,
	        right: '32%',
	        roam: true,
	        label: {
	            emphasis: {
	                show: false
	            }
	        },
	        itemStyle: {
	            normal: {
	                areaColor: areaColor,
	                borderColor: borderColor
	            },
	            emphasis: {
	                areaColor: '#2a333d'
	            }
	        }
	    },
	    grid: {
	        right: 50,
	        top: 30,
	        bottom: '20%',
	        width: '28%'
	    },
	    xAxis: {
	        type: 'value',
	        min: 0,
	        scale: true,
	        position: 'top',
	        boundaryGap: false,
	        splitLine: {show: false},
	        axisLine: {show: false},
	        axisTick: {show: false},
	        axisLabel: {show: false, margin: 2, textStyle: {color: '#aaa'}}
	    },
	    yAxis: {
	        type: 'category',
	        axisLine: {show: false, lineStyle: {color: '#ddd'}},
	        axisTick: {show: false, lineStyle: {color: '#ddd'}},
	        axisLabel: {inside: true,interval: 0, textStyle: {fontSize: 16,color: '#ffffff'}},
	        zlevel: 3,
	        data: []
	    },
	    series : [
	        {
	            name: 'main',
	            type: 'scatter',
	            coordinateSystem: 'geo'
	        },
	        {
	            name: 'content',
	            zlevel: 2,
	            type: 'bar',
	            symbol: 'none',
	            itemStyle: {
	                normal: {
	                    color: '#3B6C88', // #356aa0
	                    barBorderWidth: 0,
				        shadowBlur: 10,
				        shadowOffsetX: 3,
				        shadowOffsetY: 3,
				        shadowColor: '#000000'
	                }
	            }
	        }
	    ]
	};



    var interval = 10;
    var time = 0;

	function fetchStreamingStartTime() {
	    var getUrl = "common/stream_getStreamStartTime"
	    $.ajax({
	        async:false,
	        url:getUrl,
	        type:"get",
	        dataType:"json",
	        success:function(data) {
                time=data.start_time;
	        }
	    });
	}

	//fetchStreamingStartTime();
    function shopRankList() {

	    var shop = [];
	    var trade = [];

        var getUrl = "common/query_getMerchantTrade";
        $.ajax({
            async:false,
            url:getUrl,
            type:"get",
            dataType:"json",
            success:function(data) {
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
                show: true,
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
                            color: function(params) {
                                // build a color map as your need.
                                var colorList = [
                                    '#C1232B','#B5C334','#FCCE10','#E87C25','#27727B',
                                    '#FE8463','#9BCA63','#FAD860','#F3A43B','#60C0DD',
                                    '#D7504B','#C6E579','#F4E001','#F0805A','#26C0C0'
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
                            formatter: function(params){
                                return '<img src="'
                                    + params.data.symbol.replace('image://', '')
                                    + '"/>';
                            }
                        },
                        data: [
                            {xAxis:0, y: 350, name:'Line', symbolSize:20, symbol: 'image://../asset/ico/折线图.png'},
                            {xAxis:1, y: 350, name:'Bar', symbolSize:20, symbol: 'image://../asset/ico/柱状图.png'},
                            {xAxis:2, y: 350, name:'Scatter', symbolSize:20, symbol: 'image://../asset/ico/散点图.png'},
                            {xAxis:3, y: 350, name:'K', symbolSize:20, symbol: 'image://../asset/ico/K线图.png'},
                            {xAxis:4, y: 350, name:'Pie', symbolSize:20, symbol: 'image://../asset/ico/饼状图.png'},
                            {xAxis:5, y: 350, name:'Radar', symbolSize:20, symbol: 'image://../asset/ico/雷达图.png'},
                            {xAxis:6, y: 350, name:'Chord', symbolSize:20, symbol: 'image://../asset/ico/和弦图.png'},
                            {xAxis:7, y: 350, name:'Force', symbolSize:20, symbol: 'image://../asset/ico/力导向图.png'},
                            {xAxis:8, y: 350, name:'Map', symbolSize:20, symbol: 'image://../asset/ico/地图.png'},
                            {xAxis:9, y: 350, name:'Gauge', symbolSize:20, symbol: 'image://../asset/ico/仪表盘.png'},
                            {xAxis:10, y: 350, name:'Funnel', symbolSize:20, symbol: 'image://../asset/ico/漏斗图.png'},
                        ]
                    }
                }
            ]
        };

        myChartHead.setOption(option_head);
    }

    shopRankList();
	//定时刷新 定位毫秒
    setInterval(shopRankList, interval * 1000);


	function ajaxQuery() {
		var getUrl = "common/query_?time=" + time;
		time++;
		
	    $.get({url:getUrl}).done(function(data) {
	    	var area = data.area;
			var content = data.content;
			
			var max = 0;
			area.forEach(function (itemOpt) {
			    if (itemOpt.value > max) {
			        max = itemOpt.value;
			    }
			});
			
			if(max == 0) {
				return ;
			}
			
			/*
			myChart.setOption({
		        series: [{
		        	name: 'main',
		            data: []
		        }]
		    })*/
			
		    myChart.setOption({
		    	visualMap: {
			        max: max
			    },
			    yAxis: {
		    		data: convertData(content,['title','value'])
	    		},
		        series: [{
		            name: 'main',
		            data: area.map(function (itemOpt) {
		            	var itemCoord = getCoordMapItem(itemOpt.name);
		            	if(itemCoord == undefined) {
		            		return ;
		            	}
		            	var position = itemCoord.position == undefined ? 'top' : itemCoord.position;
		                return {
		                    name: itemOpt.name,
		                    // symbol: "pin",
		                    value: [
		                        itemCoord.longitude,
		                        itemCoord.latitude,
		                        itemOpt.value
		                    ],
		                    dimeId: itemOpt.dimeId,
		                    label: {
		                    	normal: {
		                    		show: true,
		                    		position:position,
		                    		formatter: function (param) {
					                    return param.data['name'] + ":" + param.data['value'][2];
					                },
					                textStyle: {
					                	fontSize: 16
					                }
		                        },
		                        emphasis: {
		                            show: true,
		                    		position:position,
		                    		formatter: function (param) {
					                    return param.data['name'] + ":" + param.data['value'][2];
					                },
					                textStyle: {
					                	fontSize: 20
					                }
		                        }
		                    },
		                    itemStyle: {
		                        normal: {
		                            color: itemCoord.color
		                        }
		                    }
		                };
		            })
		        },{
		            name: 'content',
		            data: convertData(content,['url','value'])
		        }]
		    });
		});
	}
	
	//ajaxQuery();
    //定时任务 刷新地图
	//setInterval(ajaxQuery, interval * 1000);
    
	myChart.setOption(option);

	myChart.on('click', function (params) {
		var seriesType = params.seriesType;
		if(seriesType == 'bar') {
			var url = params.data.url;
			window.open(url);
		}
	});
    </script>
  </body>
</html>