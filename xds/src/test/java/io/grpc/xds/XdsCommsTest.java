/*
 * Copyright 2019 The gRPC Authors
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

package io.grpc.xds;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.UInt32Value;
import io.envoyproxy.envoy.api.v2.Cluster;
import io.envoyproxy.envoy.api.v2.Cluster.DiscoveryType;
import io.envoyproxy.envoy.api.v2.Cluster.LbPolicy;
import io.envoyproxy.envoy.api.v2.ClusterLoadAssignment;
import io.envoyproxy.envoy.api.v2.DiscoveryRequest;
import io.envoyproxy.envoy.api.v2.DiscoveryResponse;
import io.envoyproxy.envoy.api.v2.core.Address;
import io.envoyproxy.envoy.api.v2.core.Locality;
import io.envoyproxy.envoy.api.v2.core.SocketAddress;
import io.envoyproxy.envoy.api.v2.endpoint.Endpoint;
import io.envoyproxy.envoy.api.v2.endpoint.LbEndpoint;
import io.envoyproxy.envoy.api.v2.endpoint.LocalityLbEndpoints;
import io.envoyproxy.envoy.service.discovery.v2.AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceImplBase;
import io.grpc.LoadBalancer.Helper;
import io.grpc.LoadBalancerProvider;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.testing.StreamRecorder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.xds.XdsComms.AdsStreamCallback;
import io.grpc.xds.XdsLbState.LocalityInfo;
import io.grpc.xds.XdsLbState.SubchannelStore;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link XdsComms}.
 */
@RunWith(JUnit4.class)
public class XdsCommsTest {
  @Rule
  public final GrpcCleanupRule cleanupRule = new GrpcCleanupRule();
  @Mock
  private Helper helper;
  @Mock
  private AdsStreamCallback adsStreamCallback;
  @Mock
  private SubchannelStore subchannelStore;
  @Captor
  private ArgumentCaptor<Map<XdsLbState.Locality, LocalityInfo>> localityEndpointsMappingCaptor;

  private final SynchronizationContext syncContext = new SynchronizationContext(
      new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          throw new AssertionError(e);
        }
      });

  private final StreamRecorder<DiscoveryRequest> streamRecorder = StreamRecorder.create();
  private StreamObserver<DiscoveryResponse> responseWriter;

  private ManagedChannel channel;
  private XdsComms xdsComms;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    String serverName = InProcessServerBuilder.generateName();

    AggregatedDiscoveryServiceImplBase serviceImpl = new AggregatedDiscoveryServiceImplBase() {
      @Override
      public StreamObserver<DiscoveryRequest> streamAggregatedResources(
          final StreamObserver<DiscoveryResponse> responseObserver) {
        responseWriter = responseObserver;

        return new StreamObserver<DiscoveryRequest>() {

          @Override
          public void onNext(DiscoveryRequest value) {
            streamRecorder.onNext(value);
          }

          @Override
          public void onError(Throwable t) {
            streamRecorder.onError(t);
          }

          @Override
          public void onCompleted() {
            streamRecorder.onCompleted();
            responseObserver.onCompleted();
          }
        };
      }
    };

    cleanupRule.register(
        InProcessServerBuilder
            .forName(serverName)
            .addService(serviceImpl)
            .directExecutor()
            .build()
            .start());
    channel =
        cleanupRule.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());
    doReturn("fake_authority").when(helper).getAuthority();
    doReturn(syncContext).when(helper).getSynchronizationContext();
    xdsComms = new XdsComms(channel, helper, adsStreamCallback, subchannelStore);
  }

  @Test
  public void shutdownLbComm() throws Exception {
    xdsComms.shutdownChannel();
    assertTrue(channel.isShutdown());
    assertTrue(streamRecorder.awaitCompletion(1, TimeUnit.SECONDS));
    assertEquals(Status.Code.CANCELLED, Status.fromThrowable(streamRecorder.getError()).getCode());
  }

  @Test
  public void shutdownLbRpc_verifyChannelNotShutdown() throws Exception {
    xdsComms.shutdownLbRpc("shutdown msg1");
    assertTrue(streamRecorder.awaitCompletion(1, TimeUnit.SECONDS));
    assertEquals(Status.Code.CANCELLED, Status.fromThrowable(streamRecorder.getError()).getCode());
    assertFalse(channel.isShutdown());
  }

  @Test
  public void cancel() throws Exception {
    xdsComms.shutdownLbRpc("cause1");
    assertTrue(streamRecorder.awaitCompletion(1, TimeUnit.SECONDS));
    assertEquals(Status.Code.CANCELLED, Status.fromThrowable(streamRecorder.getError()).getCode());
  }

  @Test
  public void standardMode_sendCdsRequest_getCdsResponse_sendEdsRequest_getEdsResponse() {
    assertThat(streamRecorder.getValues()).hasSize(1);
    DiscoveryRequest request = streamRecorder.getValues().get(0);
    assertThat(request.getTypeUrl()).isEqualTo("type.googleapis.com/envoy.api.v2.Cluster");
    assertThat(request.getResourceNames(0)).isEqualTo("fake_authority");

    DiscoveryResponse cdsResponse = DiscoveryResponse.newBuilder()
        .addResources(Any.pack(Cluster.newBuilder()
            .setType(DiscoveryType.EDS)
            .setLbPolicy(LbPolicy.ROUND_ROBIN)
            .build()))
        .setTypeUrl("type.googleapis.com/envoy.api.v2.Cluster")
        .build();
    responseWriter.onNext(cdsResponse);

    ArgumentCaptor<LoadBalancerProvider> lbProviderCaptor =
        ArgumentCaptor.forClass(LoadBalancerProvider.class);
    verify(subchannelStore).updateLoadBalancerProvider(lbProviderCaptor.capture());
    assertThat(lbProviderCaptor.getValue().getPolicyName()).isEqualTo("round_robin");

    assertThat(streamRecorder.getValues()).hasSize(2);
    request = streamRecorder.getValues().get(1);
    assertThat(request.getTypeUrl())
        .isEqualTo("type.googleapis.com/envoy.api.v2.ClusterLoadAssignment");
    assertThat(
            request.getNode().getMetadata().getFieldsOrThrow("endpoints_required").getBoolValue())
        .isTrue();

    Locality localityProto1 = Locality.newBuilder()
        .setRegion("region1").setZone("zone1").setSubZone("subzone1").build();
    LbEndpoint endpoint11 = LbEndpoint.newBuilder()
        .setEndpoint(Endpoint.newBuilder()
            .setAddress(Address.newBuilder()
                .setSocketAddress(SocketAddress.newBuilder()
                    .setAddress("addr11").setPortValue(11))))
        .setLoadBalancingWeight(UInt32Value.of(11))
        .build();
    LbEndpoint endpoint12 = LbEndpoint.newBuilder()
        .setEndpoint(Endpoint.newBuilder()
            .setAddress(Address.newBuilder()
                .setSocketAddress(SocketAddress.newBuilder()
                    .setAddress("addr12").setPortValue(12))))
        .setLoadBalancingWeight(UInt32Value.of(12))
        .build();
    Locality localityProto2 = Locality.newBuilder()
        .setRegion("region2").setZone("zone2").setSubZone("subzone2").build();
    LbEndpoint endpoint21 = LbEndpoint.newBuilder()
        .setEndpoint(Endpoint.newBuilder()
            .setAddress(Address.newBuilder()
                .setSocketAddress(SocketAddress.newBuilder()
                    .setAddress("addr21").setPortValue(21))))
        .setLoadBalancingWeight(UInt32Value.of(21))
        .build();
    LbEndpoint endpoint22 = LbEndpoint.newBuilder()
        .setEndpoint(Endpoint.newBuilder()
            .setAddress(Address.newBuilder()
                .setSocketAddress(SocketAddress.newBuilder()
                    .setAddress("addr22").setPortValue(22))))
        .setLoadBalancingWeight(UInt32Value.of(22))
        .build();
    DiscoveryResponse edsResponse = DiscoveryResponse.newBuilder()
        .addResources(Any.pack(ClusterLoadAssignment.newBuilder()
            .addEndpoints(LocalityLbEndpoints.newBuilder()
                .setLocality(localityProto1)
                .addLbEndpoints(endpoint11)
                .addLbEndpoints(endpoint12)
                .setLoadBalancingWeight(UInt32Value.of(1)))
            .addEndpoints(LocalityLbEndpoints.newBuilder()
                .setLocality(localityProto2)
                .addLbEndpoints(endpoint21)
                .addLbEndpoints(endpoint22)
                .setLoadBalancingWeight(UInt32Value.of(2)))
            .build()))
        .setTypeUrl("type.googleapis.com/envoy.api.v2.ClusterLoadAssignment")
        .build();
    responseWriter.onNext(edsResponse);

    XdsLbState.Locality locality1 = new XdsLbState.Locality(localityProto1);
    LocalityInfo localityInfo1 = new LocalityInfo(
        ImmutableList.of(
            new XdsLbState.LbEndpoint(endpoint11),
            new XdsLbState.LbEndpoint(endpoint12)),
        1);
    LocalityInfo localityInfo2 = new LocalityInfo(
        ImmutableList.of(
            new XdsLbState.LbEndpoint(endpoint21),
            new XdsLbState.LbEndpoint(endpoint22)),
        2);
    XdsLbState.Locality locality2 = new XdsLbState.Locality(localityProto2);

    verify(subchannelStore).updateLocalityStore(localityEndpointsMappingCaptor.capture());
    assertThat(localityEndpointsMappingCaptor.getValue()).containsExactly(
        locality1, localityInfo1, locality2, localityInfo2).inOrder();


    edsResponse = DiscoveryResponse.newBuilder()
        .addResources(Any.pack(ClusterLoadAssignment.newBuilder()
            .addEndpoints(LocalityLbEndpoints.newBuilder()
                .setLocality(localityProto2)
                .addLbEndpoints(endpoint21)
                .addLbEndpoints(endpoint22)
                .setLoadBalancingWeight(UInt32Value.of(2)))
            .addEndpoints(LocalityLbEndpoints.newBuilder()
                .setLocality(localityProto1)
                .addLbEndpoints(endpoint11)
                .addLbEndpoints(endpoint12)
                .setLoadBalancingWeight(UInt32Value.of(1)))
            .build()))
        .setTypeUrl("type.googleapis.com/envoy.api.v2.ClusterLoadAssignment")
        .build();
    responseWriter.onNext(edsResponse);

    verify(subchannelStore, times(2)).updateLocalityStore(localityEndpointsMappingCaptor.capture());
    assertThat(localityEndpointsMappingCaptor.getValue()).containsExactly(
        locality2, localityInfo2, locality1, localityInfo1).inOrder();

    xdsComms.shutdownChannel();
  }
}
