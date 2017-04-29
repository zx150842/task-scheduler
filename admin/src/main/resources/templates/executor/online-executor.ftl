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
                执行节点查看
            </h1>
        </section>

        <!-- Main content -->
        <section class="content">
            <div class="row">
                <div class="col-xs-12">

                    <div class="box">
                        <div class="box-header row">
                            <div class="col-md-10"><h3 class="box-title">online节点列表</h3></div>
                            <div class="col-md-2"><button type="button" class="btn btn-sm btn-danger" onclick="refreshWorker()">刷新节点</button></div>
                        </div>
                        <!-- /.box-header -->
                        <div class="box-body">
                            <table id="data_list" class="table table-bordered table-striped">
                                <thead>
                                <tr>
                                    <th>序号</th>
                                    <th>节点组</th>
                                    <th>地址</th>
                                    <th>节点id</th>
                                </tr>
                                </thead>
                                <tbody>
                                <#assign index=0>
                                <#list list as item>
                                    <#assign index=index+1>
                                <tr>
                                    <td>${index}</td>
                                    <td>${item.workerGroup!}</td>
                                    <td>${item.host!}:${item.port?c}</td>
                                    <td>${item.workerId!}</td>
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
        $("#executorMenu li").removeClass("active");
        $("#executorMenu").addClass("active");
        $("#onlineExecutorMenu").addClass("active");
    }

    function refreshWorker() {
        $.ajax({
            url:'/executor/refresh',
            type:'POST',
            dataType:'json',
            data:{},
            success:function(data){
                if (typeof(data) != 'undefined' && data == '0') {
                    alert("刷新成功")
                    window.location.reload()
                } else {
                    if (data == '-1') {
                        alert("刷新失败")
                    } else {
                        alert(data);
                    }
                }
            },
            error:function(data){
                alert(data)
            }
        })
    }
</script>
</body>
</html>
