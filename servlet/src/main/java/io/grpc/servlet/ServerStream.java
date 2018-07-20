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

import static io.grpc.internal.GrpcUtil.CONTENT_TYPE_GRPC;
import static io.grpc.internal.GrpcUtil.CONTENT_TYPE_KEY;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.AbstractServerStream;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.TransportFrameUtil;
import io.grpc.internal.TransportTracer;
import io.grpc.internal.WritableBuffer;
import io.grpc.internal.WritableBufferAllocator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

class ServerStream extends AbstractServerStream {

  final TransportState state =
      new TransportState(Integer.MAX_VALUE, StatsTraceContext.NOOP, new TransportTracer());
  final Sink sink;
  final AsyncContext asyncCtx;

  protected ServerStream(WritableBufferAllocator bufferAllocator, AsyncContext asyncCtx) {
    super(bufferAllocator, StatsTraceContext.NOOP);
    this.asyncCtx = asyncCtx;
    this.sink = new Sink((HttpServletResponse) asyncCtx.getResponse());
  }

  @Override
  protected TransportState transportState() {
    return state;
  }

  @Override
  protected Sink abstractServerStreamSink() {
    return sink;
  }

  static final class TransportState extends io.grpc.internal.AbstractServerStream.TransportState {

    TransportState(
        int maxMessageSize, StatsTraceContext statsTraceCtx, TransportTracer transportTracer) {
      super(maxMessageSize, statsTraceCtx, transportTracer);
    }

    @Override
    public void runOnTransportThread(Runnable r) {
      // TODO: ?
      r.run();
    }

    @Override
    public void bytesRead(int numBytes) {
      // no-op
      // TODO: flow control
    }

    @Override
    public void deframeFailed(Throwable cause) {
      // TODO
    }

    // just make it public
    @Override
    public void inboundDataReceived(ReadableBuffer frame, boolean endOfStream) {
      super.inboundDataReceived(frame, endOfStream);
    }
  }

  private static final class TrailerSupplier implements Supplier<Map<String, String>> {
    final Map<String, String> trailers = new ConcurrentHashMap<>();

    TrailerSupplier() {}

    @Override
    public Map<String, String> get() {
      return trailers;
    }
  }

  private final class Sink implements AbstractServerStream.Sink {
    final HttpServletResponse resp;

    Sink(HttpServletResponse resp) {
      this.resp = resp;
    }

    final TrailerSupplier trailerSupplier = new TrailerSupplier();

    @Override
    public void writeHeaders(Metadata headers) {
      System.out.println("writing headers");
      // Discard any application supplied duplicates of the reserved headers
      headers.discardAll(CONTENT_TYPE_KEY);
      headers.discardAll(GrpcUtil.TE_HEADER);
      headers.discardAll(GrpcUtil.USER_AGENT_KEY);

      resp.setStatus(HttpServletResponse.SC_OK);
      resp.setContentType(CONTENT_TYPE_GRPC);

      byte[][] serializedHeaders = TransportFrameUtil.toHttp2Headers(headers);
      for (int i = 0; i < serializedHeaders.length; i += 2) {
        resp.setHeader(
            new String(serializedHeaders[i], StandardCharsets.US_ASCII),
            new String(serializedHeaders[i + 1], StandardCharsets.US_ASCII));
      }
      // resp.setHeader("trailer", "grpc-status"); // , grpc-message");
      trailerSupplier.get().put("grpc-status", "0");

      resp.setTrailerFields(trailerSupplier);
      flush();
    }

    @Override
    public void writeFrame(@Nullable WritableBuffer frame, boolean flush, int numMessages) {
      System.out.println("writing frame, numMessages: " + numMessages);
      if (flush) {
        flush();
      }
      // TODO:
    }

    @Override
    public void writeTrailers(Metadata trailers, boolean headersSent, Status status) {
      // TODO:
      System.out.println("writing trailers, status: " + status);

      if (!headersSent) {
        System.out.println("writing trailers - headers not sent");
        // Discard any application supplied duplicates of the reserved headers
        trailers.discardAll(CONTENT_TYPE_KEY);
        trailers.discardAll(GrpcUtil.TE_HEADER);
        trailers.discardAll(GrpcUtil.USER_AGENT_KEY);

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType(CONTENT_TYPE_GRPC);

        byte[][] serializedHeaders = TransportFrameUtil.toHttp2Headers(trailers);
        for (int i = 0; i < serializedHeaders.length; i += 2) {
          resp.setHeader(
              new String(serializedHeaders[i], StandardCharsets.US_ASCII),
              new String(serializedHeaders[i + 1], StandardCharsets.US_ASCII));
        }
      } else {
        byte[][] serializedHeaders = TransportFrameUtil.toHttp2Headers(trailers);
        IntStream.range(0, serializedHeaders.length)
            .filter(i -> i % 2 == 0)
            .forEach(i ->
                trailerSupplier.get().put(
                    new String(serializedHeaders[i], StandardCharsets.US_ASCII),
                    new String(serializedHeaders[i + 1], StandardCharsets.US_ASCII)));
        System.out.println("setting trailers");
      }
      System.out.println("committing");

      try {
        // Glassfish workaround only:
        // otherwise client may receive Encountered end-of-stream mid-frame fo
        // r server/bidi streaming
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // ignore
      }
      asyncCtx.complete();
    }

    @Override
    public void request(int numMessages) {
      transportState()
          .runOnTransportThread(() -> transportState().requestMessagesFromDeframer(numMessages));
    }

    @Override
    public void cancel(Status status) {
      // TODO:
    }

    private void flush() {
      try {
        resp.flushBuffer();
      } catch (IOException e) {
        // TODO:
      }
    }
  }
}
