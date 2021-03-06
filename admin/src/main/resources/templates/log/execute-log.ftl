<!DOCTYPE html>
<html>
<#include "../common/header.ftl">
<body class="hold-transition skin-blue sidebar-mini">
<div class="wrapper">

<#include "../common/sidebar.ftl">

    <!-- Content Wrapper. Contains page content -->
    <div class="content-wrapper">
        <!-- Content Header (Page header) -->
        <section class="content-header">
            <h1>
                日志查看
            </h1>
        </section>

        <!-- Main content -->
        <section class="content">
            <div class="row">
                <div class="col-xs-12">

                    <div class="box">
                        <div class="box-header">
                            <h3 class="box-title">执行中任务日志</h3>
                        </div>
                        <!-- /.box-header -->
                        <div class="box-body">
                            <table id="data_list" class="table table-bordered table-striped">
                                <thead>
                                <tr>
                                    <th>序号</th>
                                    <th>任务ID</th>
                                    <th>任务名称</th>
                                    <th>执行节点组</th>
                                    <th>任务运行ID</th>
                                    <th>任务参数</th>
                                    <th>执行节点ID</th>
                                    <th>任务触发时间</th>
                                    <th>开始执行时间</th>
                                    <th>是否为手动触发</th>
                                </tr>
                                </thead>
                                <tbody>
                                <#assign index=0>
                                <#list list as item>
                                    <#assign index=index+1>
                                <tr>
                                    <td>${index}</td>
                                    <td>${item.taskId!}</td>
                                    <td>${item.taskName!}</td>
                                    <td>${item.workerGroup!}</td>
                                    <td>${item.sysId!}</td>
                                    <td>${item.params!}</td>
                                    <td>${item.workerId!}</td>
                                    <td>${item.executableTime!}</td>
                                    <td>${item.executingTime!}</td>
                                    <td><#if item.manualTrigger == true>是<#else>否</#if></td>
                                </tr>
                                </#list>
                            </table>
                        </div>
                        <!-- /.box-body -->
                    </div>
                    <!-- /.box -->
                </div>
                <!-- /.col -->
            </div>

        </section>
        <!-- /.content -->
    </div>
    <!-- /.content-wrapper -->
    <div class="control-sidebar-bg"></div>
</div>

<#include "../common/page.ftl">
<script>
    window.onload = function() {
        $("#sideMenu li").removeClass("active");
        $("#logMenu li").removeClass("active");
        $("#logMenu").addClass("active");
        $("#executeLogMenu").addClass("active");
    }
</script>
</body>
</html>
