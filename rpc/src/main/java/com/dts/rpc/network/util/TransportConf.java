package com.dts.rpc.network.util;

/**
 * Created by zhangxin on 2016/11/26.
 */
public class TransportConf {

  private final String MODE_KEY;
  private final String PREFERDIRECTBUFS_KEY;
  private final String CONNECTIONTIMEOUT_KEY;
  private final String BACKLOG_KEY;
  private final String NUMCONNNECTIONSPERPEER_KEY;
  private final String SERVERTHREADS_KEY;
  private final String CLIENTTHREADS_KEY;
  private final String RECEIVEBUFFER_KEY;
  private final String SENDBUFFER_KEY;
  private final String SASL_TIMEOUT_KEY;
  private final String MAXRETRIES_KEY;
  private final String RETRYWAIT_KEY;
  private final String LAZYFD_KEY;

  private final String module;

  private final ConfigProvider conf;

  public TransportConf(String module, ConfigProvider conf) {
    this.module = module;
    this.conf = conf;
    MODE_KEY = getConfKey("mode");
    PREFERDIRECTBUFS_KEY = getConfKey("preferDirectBufs");
    CONNECTIONTIMEOUT_KEY = getConfKey("connectionTimeout");
    BACKLOG_KEY = getConfKey("backLog");
    NUMCONNNECTIONSPERPEER_KEY = getConfKey("numConnectionsPerPeer");
    SERVERTHREADS_KEY = getConfKey("serverThreads");
    CLIENTTHREADS_KEY = getConfKey("clientThreads");
    RECEIVEBUFFER_KEY = getConfKey("receiveBuffer");
    SENDBUFFER_KEY = getConfKey("sendBuffer");
    SASL_TIMEOUT_KEY = getConfKey("sasl.timeout");
    MAXRETRIES_KEY = getConfKey("maxRetries");
    RETRYWAIT_KEY = getConfKey("retryWait");
    LAZYFD_KEY = getConfKey("lazyFD");
  }

  private String getConfKey(String suffix) {
    return module + "." + suffix;
  }

  public String ioMode() {
    return conf.get(MODE_KEY, "NIO").toUpperCase();
  }

  public boolean preferDirectBufs() {
    return conf.getBoolean(PREFERDIRECTBUFS_KEY, true);
  }

  public int connectionTimeoutMs() {
    return conf.getInt(CONNECTIONTIMEOUT_KEY, 120 * 1000);
  }

  public int numConnectionsPerPeer() {
    return conf.getInt(NUMCONNNECTIONSPERPEER_KEY, 1);
  }

  public int backLog() {
    return conf.getInt(BACKLOG_KEY, -1);
  }

  public int serverThreads() {
    return conf.getInt(SERVERTHREADS_KEY, 0);
  }

  public int clientThreads() {
    return conf.getInt(CLIENTTHREADS_KEY, 0);
  }

  public int receiveBuf() {
    return conf.getInt(RECEIVEBUFFER_KEY, -1);
  }

  public int sendBuf() {
    return conf.getInt(SENDBUFFER_KEY, -1);
  }

  public int saslRTTimeoutMs() {
    return conf.getInt(SASL_TIMEOUT_KEY, 30 * 1000);
  }

  public int maxIORetries() {
    return conf.getInt(MAXRETRIES_KEY, 3);
  }

  public int ioRetryWaitTimeMs() {
    return conf.getInt(RETRYWAIT_KEY, 5 * 1000);
  }

  public boolean lazyFileDescriptor() {
    return conf.getBoolean(LAZYFD_KEY, true);
  }
}
