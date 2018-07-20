/*
 * Copyright 2018 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.servlet;

import static com.google.common.base.Preconditions.checkArgument;

import io.grpc.Metadata;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.ReadableBuffers;
import io.grpc.internal.ServerTransportListener;
import io.grpc.internal.WritableBuffer;
import io.grpc.internal.WritableBufferAllocator;
import io.grpc.servlet.ServerBuilder.ServerImpl;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public final class ServletAdapter {
  private final ServerTransportListener transportListener;

  private ServletAdapter(ServerTransportListener transportListener) {
    this.transportListener = transportListener;
  }

  public void doGet(HttpServletRequest req, HttpServletResponse resp) {
    // TODO
  }

  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    checkArgument(isGrpc(req), "req is not a gRPC request");
    String method = req.getRequestURI().substring(1); // remove the leading "/"
    Metadata headers = new Metadata();
    AsyncContext asyncCtx = req.startAsync();

    resp = (HttpServletResponse) asyncCtx.getResponse();
    ServletOutputStream outputStream = resp.getOutputStream();

    resp.setBufferSize(0x4000);

    try {
      // Tomcat workaround only:
      // otherwise the servlet may hang for client/bidi streaming
      Thread.sleep(100);
    } catch (InterruptedException e) {
      // ignore
    }

    // TODO: this is a direct sink, need a real buffer? use WriteListener
    WritableBufferAllocator bufferAllocator =
        capacityHint ->
            new WritableBuffer() {
              @Override
              public void write(byte[] src, int srcIndex, int length) {
                try {
                  outputStream.write(src, srcIndex, length);
                } catch (IOException e) {
                  // TODO
                }
              }

              @Override
              public void write(byte b) {
                try {
                  outputStream.write(b);
                } catch (IOException e) {
                  // TODO
                }
              }

              @Override
              public int writableBytes() {
                return 0x1000000;
              }

              @Override
              public int readableBytes() {
                return 0x1000000;
              }

              @Override
              public void release() {}
            };

    ServerStream stream = new ServerStream(bufferAllocator, asyncCtx);
    transportListener.streamCreated(stream, method, headers);
    stream.transportState().onStreamAllocated();

    ServletInputStream input = asyncCtx.getRequest().getInputStream();

    System.out.println("setting listener");
    input.setReadListener(
        new ReadListener() {
          AtomicBoolean eosRead = new AtomicBoolean(); // EOS has been read by input.read(buffer)
          final byte buffer[] = new byte[4 * 1024];

          @Override
          public void onDataAvailable() throws IOException {
            System.out.println("onDataAvailable");

            if (!input.isReady()) {
              // Workaround for Undertow only:
              // https://issues.jboss.org/projects/UNDERTOW/issues/UNDERTOW-1379
              return;
            }

            do {
              int length = input.read(buffer);
              if (input.isFinished()) {
                eosRead.set(true);
                System.out.println("onDataAvailable - eos read true");
              }
              System.out.println("length = 0");
              if (length == -1) {
                stream
                    .transportState()
                    .inboundDataReceived(ReadableBuffers.wrap(new byte[] {}), true);
              } else {
                stream
                    .transportState()
                    .inboundDataReceived(
                        ReadableBuffers.wrap(Arrays.copyOf(buffer, length)), eosRead.get());
              }
            } while (input.isReady());
          }

          @Override
          public void onAllDataRead() {
            // glassfish triggers onAllDataRead twice - should be a bug
            System.out.println("onAllData - eos read: " + eosRead.get());
            if (eosRead.compareAndSet(false, true)) {
              stream
                  .transportState()
                  .inboundDataReceived(ReadableBuffers.wrap(new byte[] {}), true);
            }
          }

          @Override
          public void onError(Throwable t) {
            System.out.println("onError");
            // TODO:
          }
        });
  }

  public static boolean isGrpc(HttpServletRequest req) {
    return req.getContentType() != null
        && req.getContentType().contains(GrpcUtil.CONTENT_TYPE_GRPC);
  }

  public static final class Factory {
    public static ServletAdapter create(ServerBuilder serverBuilder) {
      ServerImpl server = serverBuilder.build().start();
      return new ServletAdapter(server.transportListener);
    }
  }
}
