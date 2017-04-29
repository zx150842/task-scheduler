CREATE TABLE `dts_cron_jobs` (
   `id` int(11) NOT NULL AUTO_INCREMENT,
   `job_id` varchar(32) NOT NULL COMMENT='后台配置的cron任务id',
   `tasks` varchar(256) NOT NULL COMMENT='cron任务配置',
   `cron_expression` varchar(256) NOT NULL COMMENT='cron表达式',
   `worker_group` varchar(32) NOT NULL COMMENT='接入的java应用id',
   `status` tinyint(2) NOT NULL DEFAULT '0' COMMENT='cron任务状态 0删除 1有效',
   `max_run_time_sec` int(11) NOT NULL DEFAULT '-1' COMMENT='cron任务最大执行时间，单位秒',
   `ctime` datetime DEFAULT NULL,
   `utime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   `last_trigger_time` timestamp NOT NULL DEFAULT '1978-01-01 00:00:01' COMMENT='cron任务最后触发时间',
   `desc` varchar(256) DEFAULT NULL COMMENT='cron任务描述',
   PRIMARY KEY (`id`),
   UNIQUE KEY `uk_job_id` (`job_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='定时任务配置表';

CREATE TABLE `dts_cron_tasks` (
   `id` int(11) NOT NULL AUTO_INCREMENT,
   `job_id` varchar(32) NOT NULL COMMENT='cron任务id',
   `task_id` varchar(32) NOT NULL COMMENT='cron任务中子任务id',
   `task_name` varchar(32) NOT NULL COMMENT='触发任务的名称',
   `worker_group` varchar(32) NOT NULL COMMENT='加入的java应用id',
   `sys_id` varchar(32) NOT NULL COMMENT='触发的任务id',
   `params` varchar(256) DEFAULT NULL COMMENT='任务参数',
   `manual_trigger` tinyint(1) NOT NULL DEFAULT '0' COMMENT='是否是手动触发 0否 1是',
   `worker_id` varchar(32) DEFAULT NULL COMMENT='运行任务的节点id',
   `status` tinyint(2) NOT NULL DEFAULT '0' COMMENT='任务执行状态，0删除 1待执行 2执行中',
   `executable_time` datetime DEFAULT NULL COMMENT='任务变成可执行状态的时间',
   `executing_time` datetime DEFAULT NULL COMMENT='任务下发执行时间',
   `ctime` datetime DEFAULT NULL,
   `utime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   `trigger_time` datetime DEFAULT NULL COMMENT='任务触发时间',
   `resumes` int(11) NOT NULL DEFAULT '0' COMMENT='任务重试次数',
   PRIMARY KEY (`id`),
   UNIQUE KEY `uk_sys_id` (`sys_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='触发任务表';

CREATE TABLE `dts_finish_tasks` (
   `id` int(11) NOT NULL AUTO_INCREMENT,
   `job_id` varchar(32) NOT NULL,
   `task_id` varchar(32) NOT NULL,
   `task_name` varchar(32) NOT NULL,
   `worker_group` varchar(32) NOT NULL,
   `sys_id` varchar(32) NOT NULL,
   `params` varchar(256) DEFAULT NULL,
   `manual_trigger` tinyint(1) NOT NULL DEFAULT '0',
   `worker_id` varchar(32) DEFAULT NULL,
   `executable_time` datetime DEFAULT NULL,
   `executing_time` datetime DEFAULT NULL,
   `finish_time` datetime DEFAULT NULL,
   `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   `execute_result` varchar(1024) DEFAULT NULL,
   `status` tinyint(4) NOT NULL DEFAULT '0' COMMENT='日志状态 1有效 0无效',
   PRIMARY KEY (`id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='完成任务表';