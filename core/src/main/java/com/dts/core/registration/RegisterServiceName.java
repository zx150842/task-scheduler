package com.dts.core.registration;

/**
 * 当前注册中心支持的两种serviceName
 * <pre>zookeeper中的默认的目录结构如下:
 * /dts
 * /dts/register
 * /dts/register/master
 * /dts/register/master/master node1
 * /dts/register/master/master node2
 * /dts/register/worker
 * /dts/register/worker/worker node1
 * /dts/register/worker/worker node2
 *
 * </pre>
 *
 * @author zhangxin
 */
public class RegisterServiceName {
  public static final String MASTER = "master";
  public static final String WORKER = "worker";
}
