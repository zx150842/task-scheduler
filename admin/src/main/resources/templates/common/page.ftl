<!-- page script -->
<!-- DataTables -->
<script src="plugins/datatables/jquery.dataTables.min.js"></script>
<script src="plugins/datatables/dataTables.bootstrap.min.js"></script>
<script>
    $(function () {
        $('#data_list').DataTable({
            "paging": true,
            "lengthChange": true,
            "searching": false,
            "ordering": false,
            "info": true,
            "autoWidth": false,
            "language" : {
                "sProcessing" : "处理中...",
                "sLengthMenu" : "每页 _MENU_ 条记录",
                "sZeroRecords" : "没有匹配结果",
                "sInfo" : "第 _PAGE_ 页 ( 总共 _PAGES_ 页 )",
                "sInfoEmpty" : "无记录",
                "sInfoFiltered" : "(由 _MAX_ 项结果过滤)",
                "sInfoPostFix" : "",
                "sSearch" : "搜索:",
                "sUrl" : "",
                "sEmptyTable" : "表中数据为空",
                "sLoadingRecords" : "载入中...",
                "sInfoThousands" : ",",
                "oPaginate" : {
                    "sFirst" : "首页",
                    "sPrevious" : "上页",
                    "sNext" : "下页",
                    "sLast" : "末页"
                },
                "oAria" : {
                    "sSortAscending" : ": 以升序排列此列",
                    "sSortDescending" : ": 以降序排列此列"
                }
            }
         });
    });
</script>

