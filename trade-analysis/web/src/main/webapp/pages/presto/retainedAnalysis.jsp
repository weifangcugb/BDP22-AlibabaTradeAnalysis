<%--
  Created by IntelliJ IDEA.
  User: ze
  Date: 13/08/19
  Time: 20:28
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%
    String path = request.getContextPath();
    String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>
<html>
<head>
    <base href="<%=basePath%>">
    <meta charset="UTF-8"/>
    <title>客户留存分析-Presto</title>
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
    <script type="text/javascript" src="pages/js/bootstrap-table-zh-CN.js"></script>
    <script type="text/javascript" src="pages/js/bootstrap-table-export.min.js"></script>
    <script>
        //获取后台数据
        function getData() {
            var startDate = $("#date_input").val();
            //startDate = startDate.replace(/-/g, '');
            $("#show_table").bootstrapTable('destroy');

            $("#show_table").bootstrapTable.prototype.getPage = function (params) {
                return {pageSize: this.options.pageSize, pageNumber: this.options.pageNumber};
            };
            $("#show_table").bootstrapTable({
                method: 'get',
                url: "common/retained_getRetainedList?startTime=" + startDate,
                dataType: "json",
                striped:true,
                cache:true,
                pagination: true,
                paginationLoop:false,
                paginationPreText:'上一页',
                paginationNextText:'下一页',
                sortable: true,
                search:false,
                pageNumber:1,
                pageSize:10,
                height: 800,
                // queryParams : null,
                exportDataType: "all",
                columns: [
                    {
                        field: 'day',
                        title: "日期",
                        align: 'center',
                        valign: 'middle'
                    }, {
                        field: 'day0',
                        align: 'center',
                        valign: 'middle',
                        title: "第0天"
                    }, {
                        field: 'day1',
                        align: 'center',
                        valign: 'middle',
                        title: "第1天"
                    }, {
                        field: 'day2',
                        align: 'center',
                        valign: 'middle',
                        title: "第2天"
                    }, {
                        field: 'day3',
                        align: 'center',
                        valign: 'middle',
                        title: "第3天"
                    }, {
                        field: 'day4',
                        align: 'center',
                        valign: 'middle',
                        title: "第4天"
                    },
                    {
                        field: 'day5',
                        align: 'center',
                        valign: 'middle',
                        title: "第5天"
                    },
                    {
                        field: 'day6',
                        align: 'center',
                        valign: 'middle',
                        title: "第6天"
                    },
                    {
                        field: 'day7',
                        align: 'center',
                        valign: 'middle',
                        title: "第7天"
                    }
                ],
                formatter: function (val, row, index) {
                    // return index+1
                    var pageSize = $('#table').bootstrapTable('destroy').pageSize;     //通过table的#id 得到每页多少条
                    var pageNumber = $('#table').bootstrapTable('destroy').pageNumber; //通过table的#id 得到当前第几页
                    return pageSize * (pageNumber - 1) + index + 1;    // 返回每条的序号： 每页条数 *（当前页 - 1 ）+ 序号
                }
            })
        }

    </script>
</head>
<body style="width:100%">
<div class="mainhead fl" id="head">
    <h4 class="fl">客户留存率分析：</h4>
</div>
<div id="wrapper">
    <div class="container-fluid" style="padding-right: 50px;padding-left: 50px;">
        <div class="panel" style="padding: 5px 0px;">
            <div class="row">
                <div class="col-md-4">
                    <label style="text-align:right;padding-top:5px;width: 30%;">查询日期：</label>
                    <div class="input-daterange input-group col-xs-4" style="float:right;width: 66.5%;" id="datepicker">
                        <input type="text" class="input-sm form-control" name="start" id="date_input"
                               readonly="readonly"/>
                    </div>
                </div>
                <div class="col-md-2">
                    <div class="btn btn-primary col-xs-4" id="search_btn">查询</div>
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
            format: "yyyy-mm",
            language: "zh-CN",
            todayHighlight: true
        }).on('changeDate', function (e) {
            var startTime = e.date;
            $('#date_input_end').datepicker('setStartDate', startTime);
        });

        //初始化日期输入框为当天日期
        var date = getDate();
        var d1 = new Date("2016-01-01");
        var d2 = new Date("2016-12-31");
        $('#date_input').datepicker('setDate', d1);
        //根据日期请求数据
        getData();
    });

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

    $("#search_btn").click(function () {
        getData();
    })
</script>
</html>
