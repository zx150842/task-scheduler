package com.dts.core.registration;

import java.util.List;
import java.util.Map;

/**
 * 注册中心回调接口
 *
 * @author zhangxin
 */
public interface ZKNodeChangeListener {

  /**
   * 监听的serviceName改变时的回调方法
   *
   * @param serviceName
   * @param messages
   */
  void onChange(String serviceName, List<RpcRegisterMessage> messages);

  /**
   * 待监听的serviceName
   *
   * @return
   */
  List<String> getListeningServiceNames();
}
