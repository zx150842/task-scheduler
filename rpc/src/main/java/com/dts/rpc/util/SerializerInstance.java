package com.dts.rpc.util;

import com.dts.rpc.DTSConf;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * @author zhangxin
 */
public class SerializerInstance {

  private int counterReset;

  public SerializerInstance(DTSConf conf) {
    this.counterReset = conf.getInt("dts.serializer.objectStreamReset", 100);
  }

  public <T> ByteBuffer serialize(T t) {
    ByteBufferOutputStream bos = new ByteBufferOutputStream();
    SerializationStream out = serializeStream(bos);
    out.writeObject(t);
    out.close();
    return bos.toByteBuffer();
  }

  public <T> T deserialize(ByteBuffer bytes) {
    ByteBufferInputStream bis = new ByteBufferInputStream(bytes);
    DeserializationStream in = deserializeStream(bis);
    return in.readObject();
  }

  public SerializationStream serializeStream(OutputStream s) {
    return new SerializationStream(s, counterReset);
  }

  public DeserializationStream deserializeStream(InputStream s) {
    return new DeserializationStream(s);
  }
}

class SerializationStream {
  private final OutputStream out;
  private final int counterReset;

  private final ObjectOutputStream objOut;
  private int counter = 0;

  public SerializationStream(OutputStream out, int counterReset) {
    this.out = out;
    this.counterReset = counterReset;
    try {
      this.objOut = new ObjectOutputStream(out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public <T> SerializationStream writeObject(T t) {
    try {
      objOut.writeObject(t);
      if (counterReset > 0 && ++counter >= counterReset) {
        objOut.reset();
        counter = 0;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public void flush() {
    try {
      objOut.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    try {
      objOut.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

class DeserializationStream {
  private final InputStream in;

  private final ObjectInputStream objIn;

  public DeserializationStream(InputStream in) {
    this.in = in;
    try {
      this.objIn = new ObjectInputStream(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public <T> T readObject() {
    try {
      return (T) objIn.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    try {
      objIn.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
