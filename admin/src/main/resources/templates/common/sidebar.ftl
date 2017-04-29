<!-- Main Header -->
<header class="main-header">

    <!-- Logo -->
    <a href="scheduler" class="logo">
        <!-- mini logo for sidebar mini 50x50 pixels -->
        <span class="logo-mini"><b>S</b>ched</span>
        <!-- logo for regular state and mobile devices -->
        <span class="logo-lg"><b>任务调度</b>管理</span>
    </a>

    <!-- Header Navbar -->
    <nav class="navbar navbar-static-top" role="navigation">
        <!-- Sidebar toggle button-->
        <a href="#" class="sidebar-toggle" data-toggle="offcanvas" role="button">
            <span class="sr-only">Toggle navigation</span>
        </a>
        <!-- Navbar Right Menu -->
        <div class="navbar-custom-menu">
            <ul class="nav navbar-nav">
                <!-- User Account Menu -->
                <li class="dropdown user user-menu">
                    <!-- Menu Toggle Button -->
                    <a href="#" class="dropdown-toggle" data-toggle="dropdown">
                        <!-- The user image in the navbar-->
                        <img src="/dist/img/sogou-icon.jpg" class="user-image" alt="User Image">
                        <!-- hidden-xs hides the username on small devices so only the image appears. -->
                        <span class="hidden-xs">欢迎使用任务调度系统</span>
                    </a>
                </li>
            </ul>
        </div>
    </nav>
</header>

<!-- Left side column. contains the logo and sidebar -->
<aside class="main-sidebar">

    <!-- sidebar: style can be found in sidebar.less -->
    <section class="sidebar">

        <!-- Sidebar Menu -->
        <ul class="sidebar-menu" id="sideMenu">
            <li class="header">菜单</li>
            <li id="jobMenu"><a href="/job"><i class="fa fa-circle-o"></i><span>任务管理</span></a></li>
            <li id="schedulerMenu"><a href="/scheduler"><i class="fa fa-circle-o"></i><span>调度节点</span></a></li>
            <li id="executorMenu" class="treeview">
                <a href="#">
                    <i class="fa fa-circle-o"></i><span>执行节点</span>
                    <span class="pull-right-container"><i class="fa fa-angle-left pull-right"></i></span>
                </a>
                <ul class="treeview-menu">
                    <li id="onlineExecutorMenu"><a href="/executor/online"><i class="fa fa-circle-o"></i>online节点</a></li>
                    <li id="seedExecutorMenu"><a href="/executor/seed"><i class="fa fa-circle-o"></i>seed节点</a> </li>
                </ul>
            </li>
            <li id="logMenu" class="treeview">
                <a href="#">
                    <i class="fa fa-circle-o"></i><span>任务日志</span>
                    <span class="pull-right-container"><i class="fa fa-angle-left pull-right"></i></span>
                </a>
                <ul class="treeview-menu">
                    <li id="executeLogMenu"><a href="/log/execute"><i class="fa fa-circle-o"></i>执行中任务日志</a></li>
                    <li id="finishLogMenu"><a href="/log/finish"><i class="fa fa-circle-o"></i>执行完成任务日志</a></li>
                </ul>
            </li>
        </ul>
        <!-- /.sidebar-menu -->
    </section>
    <!-- /.sidebar -->
</aside>
