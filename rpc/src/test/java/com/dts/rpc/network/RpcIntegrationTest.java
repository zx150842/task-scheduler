package com.dts.rpc.network;

import com.dts.rpc.network.client.RpcResponseCallback;
import com.dts.rpc.network.client.TransportClient;
import com.dts.rpc.network.client.TransportClientFactory;
import com.dts.rpc.network.server.RpcHandler;
import com.dts.rpc.network.server.TransportServer;
import com.dts.rpc.network.util.SystemPropertyConfigProvider;
import com.dts.rpc.network.util.TransportConf;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.Unpooled;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author zhangxin
 */
public class RpcIntegrationTest {
  static TransportServer server;
  static TransportClientFactory clientFactory;
  static RpcHandler rpcHandler;
  static List<String> oneWayMsgs;

  @BeforeClass
  public static void setUp() throws Exception {
    TransportConf conf = new TransportConf("rpc", new SystemPropertyConfigProvider());
    rpcHandler = new RpcHandler() {
      @Override public void receive(TransportClient client, ByteBuffer message,
        RpcResponseCallback callback) {
        String msg = Unpooled.wrappedBuffer(message).toString(StandardCharsets.UTF_8);
        String[] parts = msg.split("/");
        if (parts[0].equals("hello")) {
          callback.onSuccess(Unpooled.wrappedBuffer(("Hello, " + parts[1] + "!").getBytes(StandardCharsets.UTF_8)).nioBuffer());
        } else if (parts[0].equals("return error")) {
          callback.onFailure(new RuntimeException("Returned: " + parts[1]));
        } else if (parts[0].equals("throw error")) {
          throw new RuntimeException("Thrown: " + parts[1]);
        }
      }

      @Override public void receive(TransportClient client, ByteBuffer message) {
        oneWayMsgs.add(Unpooled.wrappedBuffer(message).toString(StandardCharsets.UTF_8));
      }
    };
    TransportContext context = new TransportContext(conf, rpcHandler);
    server = context.createServer(TestUtils.getLocalHost(), 10000);
    clientFactory = context.createClientFactory();
    oneWayMsgs = Lists.newArrayList();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    server.close();
    clientFactory.close();
  }

  static class RpcResult {
    public Set<String> successMessages;
    public Set<String> errorMessages;
  }

  private RpcResult sendRPC(String... commands) throws Exception {
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    final Semaphore sem = new Semaphore(0);

    final RpcResult res = new RpcResult();
    res.successMessages = Collections.synchronizedSet(Sets.newHashSet());
    res.errorMessages = Collections.synchronizedSet(Sets.newHashSet());

    RpcResponseCallback callback = new RpcResponseCallback() {
      @Override public void onSuccess(ByteBuffer message) {
        String response = Unpooled.wrappedBuffer(message).toString(StandardCharsets.UTF_8);
        res.successMessages.add(response);
        sem.release();
      }

      @Override public void onFailure(Throwable e) {
        res.errorMessages.add(e.getMessage());
        sem.release();
      }
    };

    for (String command : commands) {
      client.sendRpc(Unpooled.wrappedBuffer(command.getBytes(StandardCharsets.UTF_8)).nioBuffer(), callback);
    }

    if (!sem.tryAcquire(commands.length, 50, TimeUnit.SECONDS)) {
      fail("Timeout getting response from the server");
    }
    client.close();
    return res;
  }

  @Test
  public void singleRPC() throws Exception {
    RpcResult res = sendRPC("hello/Aaron");
    assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!"));
    assertTrue(res.errorMessages.isEmpty());
  }

  @Test
  public void doubleRPC() throws Exception {
    RpcResult res = sendRPC("hello/Aaron", "hello/Reynold");
    assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!", "Hello, Reynold!"));
    assertTrue(res.errorMessages.isEmpty());
  }

  @Test
  public void returnErrorRPC() throws Exception {
    RpcResult res = sendRPC("return error/OK");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Returned: OK"));
  }

  @Test
  public void throwErrorRPC() throws Exception {
    RpcResult res = sendRPC("throw error/un-oh");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: un-oh"));
  }

  @Test
  public void doubleTrouble() throws Exception {
    RpcResult res = sendRPC("return error/OK", "throw error/uh-oh");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Returned: OK", "Thrown: uh-oh"));
  }


  @Test
  public void sendSuccessAndFailure() throws Exception {
    RpcResult res = sendRPC("hello/Bob", "throw error/the", "hello/Builder", "return error/!");
    assertEquals(res.successMessages, Sets.newHashSet("Hello, Bob!", "Hello, Builder!"));
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: the", "Returned: !"));
  }

  @Test
  public void sendOneWayMessage() throws Exception {
    final String message = "no reply";
    TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
    try {
      client.send(Unpooled.wrappedBuffer(message.getBytes(StandardCharsets.UTF_8)).nioBuffer());
      assertEquals(0, client.getHandler().numOutstandingRequests());
      long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
      while (System.nanoTime() < deadline && oneWayMsgs.size() == 0) {
        TimeUnit.MILLISECONDS.sleep(10);
      }

      assertEquals(1, oneWayMsgs.size());
      assertEquals(message, oneWayMsgs.get(0));
    } finally {
      client.close();
    }
  }

  private void assertErrorsContain(Set<String> errors, Set<String> contains) {
    assertEquals(contains.size(), errors.size());
    Set<String> remainingErrors = Sets.newHashSet(errors);
    for (String contain : contains) {
      Iterator<String> it = remainingErrors.iterator();
      boolean foundMatch = false;
      while (it.hasNext()) {
        if (it.next().contains(contain)) {
          it.remove();
          foundMatch = true;
          break;
        }
      }
      assertTrue("Could not find error containing " + contain + "; errors: " + errors, foundMatch);
    }
    assertTrue(remainingErrors.isEmpty());
  }
}
