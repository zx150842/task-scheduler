<!DOCTYPE html>
<html>
<#include "common/header.ftl">
<body class="hold-transition skin-blue sidebar-mini">
<div class="wrapper">

<#include "common/sidebar.ftl">

    <div class="content-wrapper">
        <section class="content-header">
            <h1>
                任务管理
            </h1>
        </section>

        <!-- Main content -->
        <section class="content">
            <div class="row">
                <form action="/job">
                    <div class="col-xs-4">
                        <div class="input-group">
                            <span class="input-group-addon">任务组</span>
                            <select class="form-control" name="workerGroup" id="searchWorkerGroupSelector">
                                <option value="0" <#if workerGroup ?? && workerGroup == "${searchWorkerGroup!}">selected</#if>>全部</option>
                            <#list workerGroups as workerGroup>
                                <option value="${workerGroup!}" <#if workerGroup ?? && workerGroup == "${searchWorkerGroup!}">selected</#if>>${workerGroup!}</option>
                            </#list>
                            </select>
                        </div>
                    </div>
                    <div class="col-xs-4">
                        <div class="input-group">
                            <span class="input-group-addon">任务名</span>
                            <select class="form-control" name="taskName" id="searchTaskSelector">
                                <option value="0" <#if taskName ?? && taskName == "${searchTaskName!}">selected</#if>>全部</option>
                            <#if tasks ??>
                                <#list tasks as task>
                                    <option value="${task._1}" <#if taskName ?? && taskName == "${searchTaskName!}">selected</#if>>${task._1}(${task._2})</option>
                                </#list>
                            </#if>
                            </select>
                        </div>
                    </div>
                    <div class="col-xs-2">
                        <button type="submit" class="btn btn-block btn-info" id="filterBtn">搜索</button>
                    </div>
                </form>
                <div class="col-xs-2">
                    <button class="btn btn-block btn-success add" data-toggle="modal" data-target="#addModal">+新增任务</button>
                </div>
            </div>

            <div class="row">
                <div class="col-xs-12">

                    <div class="box">
                        <div class="box-header row">
                            <div class="col-md-10"><h3 class="box-title">调度节点列表</h3></div>
                            <div class="col-md-2"><button type="button" class="btn btn-sm btn-danger" onclick="refreshJob()">刷新任务</button></div>
                        </div>
                        <!-- /.box-header -->
                        <div class="box-body">
                            <table id="data_list" class="table table-bordered table-striped">
                                <thead>
                                <tr>
                                    <th>序号</th>
                                    <th>job id</th>
                                    <th>任务组</th>
                                    <th>描述</th>
                                    <th>cron</th>
                                    <th>taskName</th>
                                    <th>参数</th>
                                    <th>状态</th>
                                    <th>操作</th>
                                </tr>
                                </thead>
                                <tbody id="data_list_body">
                                <#assign index=0>
                                <#list list as item>
                                    <#assign index=index+1>
                                    <tr>
                                        <td>${index}</td>
                                        <td>${item.jobId!}</td>
                                        <td>${item.workerGroup!}</td>
                                        <td>${item.desc!}</td>
                                        <td>${item.cronExpression!}</td>
                                        <td>${item.taskName!}</td>
                                        <td>${item.params!}</td>
                                        <td>${item.status!}</td>
                                        <td>
                                            <button class="btn btn-xs btn-primary" onclick="showUpdateModal('${item.workerGroup!}','${item.taskName!}','${item.jobId!}','${item.params!}','${item.desc!}','${item.cronExpression!}','${item.taskId!}')">编辑</button>
                                            <div class="btn-group">
                                                <a class="btn btn-xs btn-primary dropdown-toggle" data-toggle="dropdown" href="#">触发<span class="caret"></span></a>
                                                <ul class="dropdown-menu">
                                                    <li><a style="cursor: pointer;" onclick="triggerJob('${item.jobId!}',true)">stage</a></li>
                                                    <li><a style="cursor: pointer;" onclick="triggerJob('${item.jobId!}',false)">online</a></li>
                                                </ul>
                                            </div>
                                            <#--<button type="button" class="btn btn-xs btn-primary" onclick="triggerJob('${item.jobId!}')">触发</button>-->
                                            <#if item.status == 1><button type="button" class="btn btn-xs btn-warning" onclick="pauseOrRunJob('${item.jobId!}', 2)">暂停</button></#if>
                                            <#if item.status == 2><button type="button" class="btn btn-xs btn-success" onclick="pauseOrRunJob('${item.jobId!}', 1)">开始</button></#if>
                                            <button type="button" class="btn btn-xs btn-danger" onclick="deleteJob('${item.jobId!}')">删除</button>
                                        </td>
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

    <!-- /.control-sidebar -->
    <div class="control-sidebar-bg"></div>
</div>

<!-- job新增.模态框 -->
<div class="modal fade" id="addModal" tabindex="-1" role="dialog"  aria-hidden="true">
    <div class="modal-dialog modal-lg">
        <div class="modal-content">
            <div class="modal-header">
                <h4 class="modal-title" >新增任务</h4>
            </div>
            <div class="modal-body">
                <form class="form-horizontal form" role="form">
                    <div class="form-group">
                        <label for="firstname" class="col-sm-2 control-label">任务组<font color="red">*</font></label>
                        <div class="col-sm-4">
                            <select class="form-control" id="addWorkerGroupSelector">
                                <#list workerGroups as workerGroup>
                                    <option value="${workerGroup}">${workerGroup}</option>
                                </#list>
                            </select>
                        </div>
                    </div>
                    <div class="form-group">
                        <label for="firstname" class="col-sm-2 control-label">任务名<font color="red">*</font></label>
                        <div class="col-sm-4">
                            <select class="form-control" id="addTaskSelector">
                                <#if firstGroupTasks ??>
                                    <#list firstGroupTasks as task>
                                        <option value="${task._1}">${task._1}(${task._2})</option>
                                    </#list>
                                </#if>
                            </select>
                        </div>
                    </div>
                    <div class="form-group">
                        <label for="firstname" class="col-sm-2 control-label">任务参数</label>
                        <div class="col-sm-4"><input type="text" class="form-control" id="addTaskParams" name="taskParams" placeholder="请输入“任务参数”" maxlength="100"></div>
                    </div>
                    <div class="form-group">
                        <label for="lastname" class="col-sm-2 control-label">任务描述<font color="red">*</font></label>
                        <div class="col-sm-4"><input type="text" class="form-control" id="addDesc" name="desc" placeholder="请输入“描述”" maxlength="50" ></div>
                    </div>
                    <div class="form-group">
                        <label for="lastname" class="col-sm-2 control-label">Cron<font color="red">*</font></label>
                        <div class="col-sm-4"><input type="text" class="form-control" id="addJobCron" name="jobCron" placeholder="请输入“Cron”" maxlength="20" ></div>
                    </div>
                    <hr>
                    <div class="form-group">
                        <div class="col-sm-offset-3 col-sm-6">
                            <button type="button" class="btn btn-primary" onclick="saveJob()">保存</button>
                            <button type="button" class="btn btn-default" data-dismiss="modal">取消</button>
                        </div>
                    </div>
                </form>
            </div>
        </div>
    </div>
</div>

<!-- 更新.模态框 -->
<div class="modal fade" id="updateModal" tabindex="-1" role="dialog" aria-hidden="true">
    <div class="modal-dialog modal-lg">
        <div class="modal-content">
            <div class="modal-header">
                <h4 class="modal-title">更新任务</h4>
            </div>
            <div class="modal-body">
                <form class="form-horizontal form" role="form">
                    <div class="form-group">
                        <label for="firstname" class="col-sm-2 control-label">任务组<font color="red">*</font></label>
                        <div class="col-sm-4"><input type="text" class="form-control" id="updateWorkerGroup" disabled="disabled"></div>
                    </div>
                    <div class="form-group">
                        <label for="firstname" class="col-sm-2 control-label">任务名<font color="red">*</font></label>
                        <div class="col-sm-4"><input type="text" class="form-control" id="updateTaskName" disabled="disabled"></div>
                    </div>
                    <div class="form-group">
                        <label for="firstname" class="col-sm-2 control-label">任务参数</label>
                        <div class="col-sm-4"><input type="text" class="form-control" id="updateTaskParams" placeholder="请输入“任务参数”" maxlength="100"></div>
                    </div>
                    <div class="form-group">
                        <label for="lastname" class="col-sm-2 control-label">任务描述<font color="red">*</font></label>
                        <div class="col-sm-4"><input type="text" class="form-control" id="updateDesc" placeholder="请输入“描述”" maxlength="50"></div>
                    </div>
                    <div class="form-group">
                        <label for="lastname" class="col-sm-2 control-label">Cron<font color="red">*</font></label>
                        <div class="col-sm-4"><input type="text" class="form-control" id="updateJobCron" placeholder="请输入“Cron”" maxlength="20"></div>
                        <div><input type="hidden" id="updateJobId"></div>
                        <div><input type="hidden" id="updateTaskId"></div>
                    </div>
                    <hr>
                    <div class="form-group">
                        <div class="col-sm-offset-3 col-sm-6">
                            <button type="button" class="btn btn-primary" onclick="updateJob()">保存</button>
                            <button type="button" class="btn btn-default" data-dismiss="modal">取消</button>
                        </div>
                    </div>
                </form>
            </div>
        </div>
    </div>
</div>
<a data-toggle="modal" data-target="#updateModal" id="updateDialog"></a>

<div class="modal fade" id="logModal">
    <div class="modal-dialog" role="document">
        <div class="modal-content">
            <div class="modal-header row">
                <div class="col-md-10"><h3 class="modal-title">手动触发任务日志</h3></div>
                <div class="col-md-2">
                    <button type="button" class="close" data-dismiss="modal" aria-label="Close">
                        <span aria-hidden="true">&times;</span>
                    </button>
                </div>
            </div>
            <div id="taskLog" class="modal-body">
            </div>
        </div>
    </div>
</div>
<a data-toggle="modal" data-target="#logModal" id="logDialog"></a>

<#include "common/page.ftl">
<script>
    window.onload = function() {
        $("#sideMenu li").removeClass("active");
        $("#jobMenu").addClass("active");
    }
    $(function() {
        $("#addWorkerGroupSelector").change(function(){
            var workerGroup = $("#addWorkerGroupSelector").val();
            $.ajax({
                url:'job/tasks',
                type:'POST',
                data:{"workerGroup":workerGroup},
                dataType:'json',
                success:function(data){
                    var html = '';
                    for (var i = 0; i < data.length; ++i) {
                        html += '<option value="'+data[i]._1+'">'+data[i]._1+'('+data[i]._2+')</option>'
                    }
                    $("#addTaskSelector").html(html);
                }
            })
        })

        $("#searchWorkerGroupSelector").change(function(){
            var workerGroup = $("#searchWorkerGroupSelector").val();
            $.ajax({
                url:'job/tasks',
                type:'POST',
                data:{"workerGroup":workerGroup},
                dataType:'json',
                success:function(data){
                    var html = '<option value="0">全部</option>';
                    for (var i = 0; i < data.length; ++i) {
                        html += '<option value="'+data[i]._1+'">'+data[i]._1+'('+data[i]._2+')</option>'
                    }
                    $("#searchTaskSelector").html(html);
                },error:function(data){
                    alert(data)
                }
            })
        })
    })

    function saveJob() {
        var workerGroup = $("#addWorkerGroupSelector").val();
        var taskName = $("#addTaskSelector").val();
        var params = $("#addTaskParams").val();
        var desc = $("#addDesc").val();
        var cron = $("#addJobCron").val();
        $.ajax({
            url:'job/add',
            type:'POST',
            dataType:'json',
            data:{
                "workerGroup":workerGroup,
                "desc":desc,
                "cronExpression":cron,
                "taskName":taskName,
                "params":params
            },
            success:function(data){
                if (typeof(data) != 'undefined' && data == '0') {
                    window.location.reload();
                } else {
                    alert(data);
                }
            },
            error:function(data){
                alert(data)
            }
       })
    }

    function showUpdateModal(workerGroup,taskName,jobId,params,desc,cron,taskId) {
        $("#updateWorkerGroup").val(workerGroup);
        $("#updateTaskName").val(taskName);
        $("#updateJobId").val(jobId);
        $("#updateTaskParams").val(params);
        $("#updateDesc").val(desc);
        $("#updateJobCron").val(cron);
        $("#updateTaskId").val(taskId)
        $("#updateDialog").click();
    }

    function updateJob() {
        var params = $("#updateTaskParams").val();
        var desc = $("#updateDesc").val();
        var cron = $("#updateJobCron").val();
        var jobId = $("#updateJobId").val();
        var workerGroup = $("#updateWorkerGroup").val();
        var taskName = $("#updateTaskName").val();
        var taskId = $("#updateTaskId").val();
        $.ajax({
            url:'job/update',
            type:'POST',
            dataType:'json',
            data:{
                "jobId":jobId,
                "workerGroup":workerGroup,
                "taskName":taskName,
                "desc":desc,
                "cronExpression":cron,
                "params":params,
                "taskId":taskId
            },
            success:function(data){
                if (typeof(data) != 'undefined' && data == '0') {
                    window.location.reload();
                } else {
                    alert(data);
                }
            },
            error:function(data){
                alert(data)
            }
        })
    }

    function pauseOrRunJob(jobId, status) {
        $.ajax({
            url:'job/pauseOrRun',
            type:'POST',
            dataType:'json',
            data:{"jobId":jobId, "status":status},
            success:function(data){
                if (typeof(data) != 'undefined' && data == '0') {
                    window.location.reload();
                } else {
                    alert(data);
                }
            },
            error:function(data){
                alert(data)
            }
        })
    }

    function refreshLog(sysId, firstShow) {
        var open = $('#logModal').hasClass('in')
        if (!firstShow && !open) {
            return
        }
        $.ajax({
            url:'job/trigger/log',
            type:'POST',
            dataType:'json',
            data:{"sysId":sysId},
            success:function(data){
                if (typeof(data) != 'undefined' && data != '') {
                    var finish = false
                    if (data.finishTime != null) {
                        finish = true
                    }
                    var html
                    if (finish) {
                        html = '<div class="row"><div class="col-md-10">任务名称：' + data.taskName + ', 任务参数：' + data.params + '</div></div>';
                    } else {
                        html = '<div class="row"><div class="col-md-10">任务名称：' + data.taskName + ', 任务参数：' + data.params + '</div><div class="col-md-2"><i class="fa fa-refresh fa-spin"></i></div></div>';
                    }
                    if (data.workerId != null) {
                        html += '<div>执行节点：' + data.workerId + '</div>'
                    }
                    if (data.executableTime != null) {
                        html += '<div>触发时间：' + new Date(parseInt(data.executableTime)).toLocaleString() + '</div>'
                    }
                    if (data.executingTime != null) {
                        html += '<div>开始执行时间：' + new Date(parseInt(data.executingTime)).toLocaleString() +　'</div>'
                    }
                    if (data.finishTime != null) {
                        html += '<div>执行完成时间：' + new Date(parseInt(data.finishTime)).toLocaleString() + '</div>'
                    }
                    if (data.executeResult != null) {
                        html += '<div>执行结果：' + data.executeResult + '</div>'
                    }
                    $("#taskLog").html(html)
                    if (!finish) {
                        setTimeout('refreshLog(' + sysId + ','+ false +')',1000)
                    }
                } else {
                    alert("刷新失败");
                }
            },
            error:function(data){
                alert("执行失败！" + data)
            }
        })
    }

    function triggerJob(jobId,runOnSeed) {
        $.ajax({
            url:'job/trigger',
            type:'POST',
            dataType:'json',
            data:{"jobId":jobId,'runOnSeed':runOnSeed},
            success:function(data){
                if (typeof(data) != 'undefined' && data != '') {
                    $("#logDialog").click();
                    refreshLog(data, true)
                } else {
                    alert("触发任务失败");
                }
            },
            error:function(data){
                alert("执行失败！" + data)
            }
        })
    }

    function deleteJob(jobId) {
        $.ajax({
            url:'job/delete',
            type:'POST',
            dataType:'json',
            data:{"jobId":jobId},
            success:function(data){
                if (typeof(data) != 'undefined' && data == '0') {
                    window.location.reload();
                } else {
                    alert(data);
                }
            },
            error:function(data){
                alert(data)
            }
       })
    }

    function refreshJob() {
        $.ajax({
            url:'job/refresh',
            type:'POST',
            dataType:'json',
                data:{},
            success:function(data){
                if (typeof(data) != 'undefined' && data == '0') {
                    alert("刷新成功")
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
