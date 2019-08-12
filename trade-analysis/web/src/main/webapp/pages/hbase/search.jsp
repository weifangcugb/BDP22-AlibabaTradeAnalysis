<%@ page language="java" import="java.util.*" pageEncoding="UTF-8" %>
<%
    String path = request.getContextPath();
    String basePath = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort() + path + "/";
%>

<!DOCTYPE html>
<html>
<head>
    <base href="<%=basePath%>">
    <meta charset="UTF-8"/>
    <title>历史账单查询-HBase</title>
    <link rel="stylesheet" href="pages/css/bootstrap.min.css">
    <link rel="stylesheet" href="pages/css/search.css">
    <link rel="stylesheet" href="pages/css/bootstrap-datepicker3.css"/>
    <link rel="stylesheet" href="pages/css/bootstrap-datetimepicker.min.css"/>
    <link rel="stylesheet" href="pages/css/bootstrap-table.css"/>
    <script type="text/javascript" src="pages/jquery2/jquery-2.2.3.min.js"></script>
    <script type="text/javascript" src="pages/js/bootstrap-datepicker.min.js"></script>
    <script type="text/javascript" src="pages/js/bootstrap.min.js"></script>
    <script type="text/javascript" src="pages/js/bootstrap-datetimepicker.min.js"></script>
    <script type="text/javascript" src="pages/js/tableExport.min.js"></script>
    <script type="text/javascript" src="pages/js/bootstrap-table.js"></script>
    <script type="text/javascript" src="pages/js/bootstrap-datepicker.zh-CN.min.js"></script>
    <script type="text/javascript" src="pages/js/search.js"></script>
    <script type="text/javascript" src="pages/js/bootstrap-table-zh-CN.js"></script>
    <script type="text/javascript" src="pages/js/bootstrap-table-export.min.js"></script>

    <style type="text/css">
        body {
            margin: 0 0 0 0;
            /*background-image: url('');*/
            /*background-color: #afd9ee;*/
            background-attachment: fixed;
            background-repeat: no-repeat;
            background-size: cover;
            -moz-background-size: cover;
            -webkit-background-size: cover;
        }
    </style>
</head>
<body style="width: 80%;margin-left: 10%">
<div class="mainhead fl" id="head">
    <h4 class="fl">历史账单查询：</h4>
</div>
<div id="wrapper">
    <div class="container-fluid" style="padding-right: 50px;padding-left: 50px;">
        <div style="padding: 5px 0px;margin-left: 10%;">
            <div class="row">
                <div class="col-md-4">
                    <label style="text-align: right; padding-top: 5px;width: 30%;">用户ID: </label>
                    <div class="col-xs-3" style="float:right;width: 60%;">
                        <input type="text" class="input-sm form-control" id="userId" placeholder="请输入" required>
                    </div>
                </div>
                <div class="col-md-1">
                    <div style="display: none;margin-top: 5px" id="tips">ID不能为空</div>
                </div>
                <div class="col-md-4">
                    <label style="text-align:left;padding-top:5px;width: 30%;">查询时间：</label>
                    <div class="input-daterange input-group col-xs-2" style="float:right;width: 66%;" id="datepicker">
                        <input type="text" class="input-sm form-control" name="start" id="date_input"
                               readonly="readonly"/>
                        <span class="input-group-addon">to</span>
                        <input type="text" class="input-sm form-control" name="end" id="date_input_end"
                               readonly="readonly"/>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="btn btn-primary col-xs-4" id="search_btn">查询</div>
                </div>
            </div>
        </div>
        <div style="padding: 5px 0px;margin-left: 10%;margin-top: 18px">
            <div class="row">
                <div class="col-md-4">
                    <label style="text-align: right; padding-top: 5px;width: 30%;">用户ID: </label>
                    <div class="col-xs-4" style="float:right;width: 60%;">
                        <input type="text" class="input-sm form-control" id="viewuserid" placeholder="请输入" required>
                    </div>
                </div>
                <div class="col-md-1">
                    <div style="display: none;margin-top: 5px" id="viewtips">ID均不能为空</div>
                </div>
                <div class="col-md-4">
                    <label style="text-align: left; padding-top: 5px;width: 30%;padding-left: 10px">商家ID: </label>
                    <div class="col-xs-2" style="float:right;width: 60%;">
                        <input type="text" class="input-sm form-control" id="viewshopid" placeholder="请输入" required>
                    </div>
                </div>
                <div class="col-sm-3">
                    <div class="btn btn-primary col-xs-4" id="view_btn">支付</div>
                </div>
            </div>
        </div>
        <div class="panel" style="padding: 20px 0px;">
            <div class="section-data" style="padding: 0px 5px;">
                <table data-row-style="rowStyle"
                       data-show-export="true"
                       data-pagination="true"
                       data-thead-classes="thead-light"
                       data-striped="true"
                       data-sort-name="createTime"
                       data-sort-order="desc"
                       data-sort-stable="true"
                       data-search="true"
                       data-pagination-successively-size="1"
                       id="show_table">
                </table>
            </div>
        </div>
    </div>
</div>
</body>

<script>

    $(function () {
        $('#date_input').datepicker({
            orientation: "bottom",
            autoclose: true,
            format: "yyyy-mm-dd",
            language: "zh-CN",
            todayHighlight: true
        }).on('changeDate', function (e) {
            var startTime = e.date;
            $('#date_input_end').datepicker('setStartDate', startTime);
        });

        $('#date_input_end').datepicker({
            orientation: "bottom",
            autoclose: true,
            format: "yyyy-mm-dd",
            language: "zh-CN",
            todayHighlight: true
        }).on('changeDate', function (e) {
            var endTime = e.date;
            $('#date_input').datepicker('setEndDate', endTime);
        });
        //初始化日期输入框为当天日期
        var date = getDate();
        var d1 = new Date("2016-01-01");
        var d2 = new Date("2016-12-31");
        $('#date_input').datepicker('setDate', d1);
        $('#date_input_end').datepicker('setDate', d2);
        //根据日期请求数据
        getData();
    });

    //查询数据
    $("#search_btn").click(function () {
        var userId = $("#userId").val();
        if(userId == "") {
            $("#tips").attr("style","display:block");
        }
        getData();
    });

    //提交
    $("#view_btn").click(function () {
        var viewuserid = $("#viewuserid").val();
        var viewshopid = $("#viewshopid").val();
        if(viewshopid == "" || viewuserid == "") {
            $("#viewtips").attr("style","display:block");
        }
        submit(viewuserid,viewshopid);
    });


    //选择下拉框内容
    $(".dropdown-menu li").click(function () {
        $("#dropdownMenu1").html($(this).text() + '<span class="caret"></span>');
    })

    //根据项目名称进行查询
    $("#dropdown_div select").change(function () {
        //alert($(this).val());
    })

    //获取后台数据
    function getData() {
        var userId = $("#userId").val();
        var startDate = $("#date_input").val();
        startDate = startDate.replace(/-/g, '');
        var endDate = $("#date_input_end").val();
        endDate = endDate.replace(/-/g, '');
        $("#show_table").bootstrapTable('destroy');
        $("#show_table").bootstrapTable({
            method: 'get',
            url: "common/query_getHBaseQueryList?userId=" + userId + "&startTime=" + startDate + "&endTime=" + endDate,
            // queryParams : null,
            exportDataType: "all",
            columns: [
                {
                    field: 'userId',
                    title: "用户ID",
                    align: 'center',
                    valign: 'middle'
                }, {
                    field: 'info.shopId',
                    align: 'center',
                    valign: 'middle',
                    title: "商家ID"
                }, {
                    field: 'info.cityName',
                    align: 'center',
                    valign: 'middle',
                    title: "所在城市"
                }, {
                    field: 'payTime',
                    align: 'center',
                    valign: 'middle',
                    title: "支付时间"
                }, {
                    field: 'info.perPay',
                    align: 'center',
                    valign: 'middle',
                    title: "人均消费"
                }, {
                    field: 'info.score',
                    align: 'center',
                    valign: 'middle',
                    title: "评分"
                },
                /*{
                    field: 'info.shopLevel',
                    align: 'center',
                    valign: 'middle',
                    title: "门店等级"
                },*/
                {
                    field: 'info.cate2Name',
                    align: 'center',
                    valign: 'middle',
                    title: "二级分类"
                }
            ]
        })
    }

    //表单提交
    function submit(viewuserid,viewshopid) {
        $.ajax({
            type: "POST",
            url:"common/query_submitTrade",
            data:{
                userid:viewuserid,
                shopid:viewshopid
            },
            dataType:"json",
            success: function(data) {
                $("#viewuserid").text("");
                $("#viewshopid").text("");
                alert("Success!")
            }
        });
    }

    //获取今天的日期 yyyyMMdd
    function getDate() {
        var d = new Date();
        var year = d.getFullYear();
        var month = d.getMonth() + 1;
        if((month+"").length <= 1) {
            month = '0' + month ;
        }
        if((day+"").length <= 1) {
            day = '0' + day;
        }
        var day = d.getDate();
        return year + ""  + month + "" + day;
    };

</script>
</html>