package com.dts.core.registration;

import java.util.List;
import java.util.Map;

/**
 * @author zhangxin
 */
public interface ZKNodeChangeListener {

  void onChange(String serviceName, List<RpcRegisterMessage> messages);

  List<String> getListeningServiceNames();
}
