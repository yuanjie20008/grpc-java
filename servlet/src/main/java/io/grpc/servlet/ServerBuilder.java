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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.grpc.Contexts.statusFromCancelled;
import static io.grpc.Status.DEADLINE_EXCEEDED;
import static io.grpc.internal.GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE;
import static io.grpc.internal.GrpcUtil.MESSAGE_ENCODING_KEY;
import static io.grpc.internal.GrpcUtil.TIMEOUT_KEY;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Attributes;
import io.grpc.BinaryLog;
import io.grpc.CompressorRegistry;
import io.grpc.Context;
import io.grpc.Decompressor;
import io.grpc.DecompressorRegistry;
import io.grpc.HandlerRegistry;
import io.grpc.InternalServerInterceptors;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerStreamTracer.Factory;
import io.grpc.Status;
import io.grpc.internal.AbstractServerImplBuilder;
import io.grpc.internal.CallTracer;
import io.grpc.internal.Channelz.SocketStats;
import io.grpc.internal.ContextRunnable;
import io.grpc.internal.FixedObjectPool;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.Instrumented;
import io.grpc.internal.InternalServer;
import io.grpc.internal.LogId;
import io.grpc.internal.ObjectPool;
import io.grpc.internal.SerializeReentrantCallsDirectExecutor;
import io.grpc.internal.SerializingExecutor;
import io.grpc.internal.ServerCallImpl;
import io.grpc.internal.ServerImpl.JumpToApplicationThreadServerStreamListener;
import io.grpc.internal.ServerListener;
import io.grpc.internal.ServerStream;
import io.grpc.internal.ServerStreamListener;
import io.grpc.internal.ServerTransport;
import io.grpc.internal.ServerTransportListener;
import io.grpc.internal.SharedResourceHolder;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.CheckForNull;
import javax.annotation.concurrent.NotThreadSafe;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Must be used in a Java EE contextual environment.
 */
@NotThreadSafe
public final class ServerBuilder extends AbstractServerImplBuilder<ServerBuilder> {
  private final BuilderParams builderParams = new BuilderParams();

  public ServerBuilder() {
    super();
  }

  @Override
  public ServerImpl build() {

    AbstractServerImplBuilder.Params abstractBuilderParams =
        new AbstractServerImplBuilder.Params(this);

    builderParams.executorPool = abstractBuilderParams.executorPool;
    builderParams.usingSharedExecutor = abstractBuilderParams.usingSharedExecutor;
    builderParams.decompressorRegistry = abstractBuilderParams.decompressorRegistry;
    builderParams.compressorRegistry = abstractBuilderParams.compressorRegistry;
    builderParams.handlerRegistry = abstractBuilderParams.handlerRegistry;
    builderParams.fallbackRegistry = abstractBuilderParams.fallbackRegistry;
    builderParams.interceptors = abstractBuilderParams.interceptors;
    builderParams.binlog = abstractBuilderParams.binlog;
    builderParams.callTracerFactory = abstractBuilderParams.callTracerFactory;

    Params params = new Params(builderParams);

    ServerImpl server = new ServerImpl(buildTransportServer(null), params);
    notifyOnBuild(server);
    return server;
  }

  @Override
  protected InternalServer buildTransportServer(List<Factory> streamTracerFactories) {
    return new InternalServerImpl();
  }

  @Override
  public ServerBuilder useTransportSecurity(File certChain, File privateKey) {
    throw new UnsupportedOperationException("TLS should be configured by the servlet container");
  }

  @Override
  public ServerBuilder maxInboundMessageSize(int bytes) {
    builderParams.maxMessageSize = bytes;
    return this;
  }

  /**
   * Provides a custom managed scheduled executor service to the server builder.
   *
   * @return this
   */
  public final ServerBuilder scheduledExecutorService(
      ScheduledExecutorService managedScheduledExecutorService) {
    builderParams.scheduledExecutorService =
        checkNotNull(managedScheduledExecutorService, "managedScheduledExecutorService");
    return this;
  }

  private static final class BuilderParams {
    int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
    @CheckForNull ScheduledExecutorService scheduledExecutorService;
    ObjectPool<? extends Executor> executorPool;
    boolean usingSharedExecutor;
    DecompressorRegistry decompressorRegistry;
    CompressorRegistry compressorRegistry;
    HandlerRegistry handlerRegistry;
    HandlerRegistry fallbackRegistry;
    List<ServerInterceptor> interceptors;
    BinaryLog binlog;
    CallTracer.Factory callTracerFactory;

    BuilderParams() {
    }
  }

  private static final class Params {
    final int maxMessageSize;
    final ScheduledExecutorService scheduledExecutorService;
    final boolean usingSharedScheduler;
    final ObjectPool<? extends Executor> executorPool;
    final boolean usingSharedExecutor;
    final DecompressorRegistry decompressorRegistry;
    final CompressorRegistry compressorRegistry;
    final HandlerRegistry handlerRegistry;
    final HandlerRegistry fallbackRegistry;
    final List<ServerInterceptor> interceptors;
    final BinaryLog binlog;
    final CallTracer callTracer;

    Params(BuilderParams builderParams) {
      this.maxMessageSize = builderParams.maxMessageSize;

      boolean usingSharedExecutor = false;
      ObjectPool<? extends Executor> executorPool = builderParams.executorPool;
      if (builderParams.usingSharedExecutor) {
        Executor executor = jsr236Executor();
        if (executor != null) {
          executorPool = new FixedObjectPool<>(executor);
        } else {
          usingSharedExecutor = true;
        }
      }
      this.executorPool = executorPool;
      this.usingSharedExecutor = usingSharedExecutor;

      boolean usingSharedScheduler = false;
      ScheduledExecutorService ses = builderParams.scheduledExecutorService;
      if (ses == null) {
        ses = jsr236Scheduler();
        if (ses == null) {
          ses = SharedResourceHolder.get(GrpcUtil.TIMER_SERVICE);
          usingSharedScheduler = true;
        }
      }
      this.scheduledExecutorService = ses;
      this.usingSharedScheduler = usingSharedScheduler;

      this.decompressorRegistry = builderParams.decompressorRegistry;
      this.compressorRegistry = builderParams.compressorRegistry;
      this.handlerRegistry = builderParams.handlerRegistry;
      this.fallbackRegistry = builderParams.fallbackRegistry;
      this.interceptors = builderParams.interceptors;
      this.binlog = builderParams.binlog;
      this.callTracer = builderParams.callTracerFactory.create();
    }

    /**
     * Looks up a container managed executor service specified by JSR 236.
     */
    @CheckForNull
    private Executor jsr236Executor() {
      @SuppressWarnings("JdkObsolete")
      Hashtable<String, String> env = new Hashtable<>();
      env.put("com.sun.jndi.ldap.connect.timeout", "5000");
      env.put("com.sun.jndi.ldap.read.timeout", "5000");
      InitialContext ctx = null;
      try {
        ctx = new InitialContext(env);
        return (ExecutorService) ctx.lookup("java:comp/DefaultManagedExecutorService");
      } catch (NamingException ignored) {
        return null;
      } finally { // so bad InitialContext is not auto closeable
        if (ctx != null) {
          try {
            ctx.close();
          } catch (NamingException ignored) {
            // ignored
          }
        }
      }
    }

    /**
     * Looks up a container managed scheduled executor service specified by JSR 236.
     */
    @CheckForNull
    private ScheduledExecutorService jsr236Scheduler() {
      @SuppressWarnings("JdkObsolete")
      Hashtable<String, String> env = new Hashtable<>();
      env.put("com.sun.jndi.ldap.connect.timeout", "5000");
      env.put("com.sun.jndi.ldap.read.timeout", "5000");
      InitialContext ctx = null;
      try {
        ctx = new InitialContext(env);
        return (ScheduledExecutorService) ctx.lookup(
            "java:comp/DefaultManagedScheduledExecutorService");
      } catch (NamingException ignored) {
        return null;
      } finally { // so bad InitialContext is not auto closeable
        if (ctx != null) {
          try {
            ctx.close();
          } catch (NamingException ignored) {
            // ignored
          }
        }
      }
    }
  }

  private static final class InternalServerImpl implements InternalServer {
    ServerListener serverListener;

    @Override
    public void start(ServerListener listener) {
      serverListener = listener;
    }

    @Override
    public void shutdown() {
      if (serverListener != null) {
        serverListener.serverShutdown();
      }
    }

    @Override
    public int getPort() {
      // port is managed by the servlet container, grpc is ignorant of that
      return -1;
    }

    @Override
    public List<Instrumented<SocketStats>> getListenSockets() {
      // sockets are managed by the servlet container, grpc is ignorant of that
      return Collections.emptyList();
    }
  }

  static final class ServerImpl extends Server {
    static final Logger log = Logger.getLogger(ServerImpl.class.getName());

    final InternalServer internalServer;
    final Params params;
    final AtomicBoolean started = new AtomicBoolean();
    final AtomicBoolean serverShutdown = new AtomicBoolean();
    final AtomicBoolean serverShutdownNow = new AtomicBoolean();
    final CountDownLatch terminationLatch = new CountDownLatch(1);
    volatile boolean terminated;
    Executor executor;

    ServerListener serverListener;
    ServerTransport serverTransport;
    ServerTransportListener transportListener;

    ServerImpl(InternalServer internalServer, Params params) {
      this.internalServer = internalServer;
      this.params = params;
    }

    @Override
    public ServerImpl start() {
      checkState(started.compareAndSet(false, true), "Already started");

      executor = params.executorPool.getObject();

      serverListener = new ServerListenerImpl();

      try {
        internalServer.start(serverListener);
      } catch (IOException e) {
        throw new IllegalStateException("Shouldn't throw any exception here", e);
      }
      // create only one transport for all requests because it has no knowledge of which request is
      // associated with which client socket. This "transport" does not do socket I/O, the container
      // does.
      serverTransport = new ServerTransportImpl();
      transportListener = serverListener.transportCreated(serverTransport);
      transportListener.transportReady(Attributes.EMPTY);
      checkState(!isShutdown(), "Shutting down");
      return this;
    }

    @Override
    public Server shutdown() {
      if (serverShutdown.compareAndSet(false, true)) {
        if (started.get()) {
          internalServer.shutdown();
        } // otherwise server can no longer be started
      }
      return this;
    }

    @Override
    public Server shutdownNow() {
      if (serverShutdownNow.compareAndSet(false, true)) {
        shutdown();
        serverTransport.shutdownNow(
            Status.UNAVAILABLE.withDescription("Server shutdownNow invoked"));
      }
      return this;
    }

    @Override
    public boolean isShutdown() {
      return serverShutdown.get();
    }

    @Override
    public boolean isTerminated() {
      return terminated;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return terminationLatch.await(timeout, unit);
    }

    @Override
    public void awaitTermination() throws InterruptedException {
      terminationLatch.await();
    }

    final class ServerListenerImpl implements ServerListener {
      ServerTransport transport;

      @Override
      public ServerTransportListener transportCreated(ServerTransport transport) {
        this.transport = transport;
        return new ServerTransportListenerImpl();
      }

      @Override
      public void serverShutdown() {
        if (transport != null) {
          transport.shutdown();
        }
      }
    }

    final class ServerTransportImpl implements ServerTransport {
      final LogId logId = LogId.allocate(getClass().getName());

      @Override
      public void shutdown() {
        // no-op
      }

      @Override
      public void shutdownNow(Status reason) {
        // TODO:
      }

      @Override
      public ScheduledExecutorService getScheduledExecutorService() {
        return params.scheduledExecutorService;
      }

      @Override
      public ListenableFuture<SocketStats> getStats() {
        // does not support instrumentation
        return null;
      }

      @Override
      public LogId getLogId() {
        return logId;
      }
    }

    final class ServerTransportListenerImpl implements  ServerTransportListener {
      @Override
      public void streamCreated(ServerStream stream, String methodName, Metadata headers) {
        // TODO:
        if (headers.containsKey(MESSAGE_ENCODING_KEY)) {
          String encoding = headers.get(MESSAGE_ENCODING_KEY);
          Decompressor decompressor = params.decompressorRegistry.lookupDecompressor(encoding);
          if (decompressor == null) {
            stream.close(
                Status.UNIMPLEMENTED.withDescription(
                    String.format("Can't find decompressor for %s", encoding)),
                new Metadata());
            return;
          }
          stream.setDecompressor(decompressor);
        }


        final Context.CancellableContext context = createContext(stream, headers);
        final Executor wrappedExecutor;
        // This is a performance optimization that avoids the synchronization and queuing overhead
        // that comes with SerializingExecutor.
        if (executor == MoreExecutors.directExecutor()) {
          wrappedExecutor = new SerializeReentrantCallsDirectExecutor();
        } else {
          wrappedExecutor = new SerializingExecutor(executor);
        }

        final JumpToApplicationThreadServerStreamListener jumpListener
            = new JumpToApplicationThreadServerStreamListener(
            wrappedExecutor, executor, stream, context);
        stream.setListener(jumpListener);
        // Run in wrappedExecutor so jumpListener.setListener() is called before any callbacks
        // are delivered, including any errors. Callbacks can still be triggered, but they will be
        // queued.

        final class StreamCreated extends ContextRunnable {

          StreamCreated() {
            super(context);
          }

          @Override
          public void runInContext() {
            ServerStreamListener listener = NOOP_LISTENER;
            try {
              ServerMethodDefinition<?, ?> method = params.handlerRegistry.lookupMethod(methodName);
              if (method == null) {
                method = params.fallbackRegistry.lookupMethod(methodName, stream.getAuthority());
              }
              if (method == null) {
                Status status = Status.UNIMPLEMENTED.withDescription(
                    "Method not found: " + methodName);
                // TODO(zhangkun83): this error may be recorded by the tracer, and if it's kept in
                // memory as a map whose key is the method name, this would allow a misbehaving
                // client to blow up the server in-memory stats storage by sending large number of
                // distinct unimplemented method
                // names. (https://github.com/grpc/grpc-java/issues/2285)
                stream.close(status, new Metadata());
                context.cancel(null);
                return;
              }
              listener = startCall(stream, methodName, method, headers, context);
            } catch (RuntimeException e) {
              stream.close(Status.fromThrowable(e), new Metadata());
              context.cancel(null);
              throw e;
            } catch (Error e) {
              stream.close(Status.fromThrowable(e), new Metadata());
              context.cancel(null);
              throw e;
            } finally {
              jumpListener.setListener(listener);
            }
          }
        }

        wrappedExecutor.execute(new StreamCreated());
      }

      /** Never returns {@code null}. */
      private <ReqT, RespT> ServerStreamListener startCall(
          ServerStream stream, String fullMethodName, ServerMethodDefinition<ReqT, RespT> methodDef,
          Metadata headers, Context.CancellableContext context) {
        ServerCallHandler<ReqT, RespT> handler = methodDef.getServerCallHandler();
        for (ServerInterceptor interceptor : params.interceptors) {
          handler = InternalServerInterceptors.interceptCallHandler(interceptor, handler);
        }
        ServerMethodDefinition<ReqT, RespT> interceptedDef =
            methodDef.withServerCallHandler(handler);
        ServerMethodDefinition<?, ?> wMethodDef = params.binlog == null
            ? interceptedDef : params.binlog.wrapMethodDefinition(interceptedDef);
        return startWrappedCall(fullMethodName, wMethodDef, stream, headers, context);
      }

      private <WReqT, WRespT> ServerStreamListener startWrappedCall(
          String fullMethodName,
          ServerMethodDefinition<WReqT, WRespT> methodDef,
          ServerStream stream,
          Metadata headers,
          Context.CancellableContext context) {
        ServerCallImpl<WReqT, WRespT> call = new ServerCallImpl<WReqT, WRespT>(
            stream,
            methodDef.getMethodDescriptor(),
            headers,
            context,
            params.decompressorRegistry,
            params.compressorRegistry,
            params.callTracer);

        ServerCall.Listener<WReqT> listener =
            methodDef.getServerCallHandler().startCall(call, headers);
        if (listener == null) {
          throw new NullPointerException(
              "startCall() returned a null listener for method " + fullMethodName);
        }
        return call.newServerStreamListener(listener);
      }

      @Override
      public Attributes transportReady(Attributes attributes) {
        return attributes;
      }

      @Override
      public void transportTerminated() {
        terminated = true;
        terminationLatch.countDown();
      }

      private Context.CancellableContext createContext(
          final ServerStream stream, Metadata headers) {
        Long timeoutNanos = headers.get(TIMEOUT_KEY);

        Context baseContext = Context.ROOT;

        if (timeoutNanos == null) {
          return baseContext.withCancellation();
        }

        Context.CancellableContext context = baseContext.withDeadlineAfter(
            timeoutNanos, NANOSECONDS, serverTransport.getScheduledExecutorService());
        final class ServerStreamCancellationListener implements Context.CancellationListener {
          @Override
          public void cancelled(Context context) {
            Status status = statusFromCancelled(context);
            if (DEADLINE_EXCEEDED.getCode().equals(status.getCode())) {
              // This should rarely get run, since the client will likely cancel the stream before
              // the timeout is reached.
              stream.cancel(status);
            }
          }
        }

        context.addListener(new ServerStreamCancellationListener(), MoreExecutors.directExecutor());

        return context;
      }
    }

    static final ServerStreamListener NOOP_LISTENER = new NoopListener();

    private static final class NoopListener implements ServerStreamListener {

      NoopListener() {}

      @Override
      public void messagesAvailable(MessageProducer producer) {
        InputStream message;
        while ((message = producer.next()) != null) {
          try {
            message.close();
          } catch (IOException e) {
            // Close any remaining messages
            while ((message = producer.next()) != null) {
              try {
                message.close();
              } catch (IOException ioException) {
                // just log additional exceptions as we are already going to throw
                log.log(Level.WARNING, "Exception closing stream", ioException);
              }
            }
            throw new RuntimeException(e);
          }
        }
      }

      @Override
      public void halfClosed() {}

      @Override
      public void closed(Status status) {}

      @Override
      public void onReady() {}
    }
  }
}