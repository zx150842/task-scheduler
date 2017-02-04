package com.dts.admin;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author zhangxin
 */
@MapperScan("com.dts.admin.dao")
@Configuration
public class MybatisConfig {
}
