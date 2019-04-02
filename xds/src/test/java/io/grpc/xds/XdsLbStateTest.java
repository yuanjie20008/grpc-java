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
import static io.grpc.ConnectivityState.CONNECTING;
import static io.grpc.ConnectivityState.READY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import io.envoyproxy.envoy.api.v2.core.HealthStatus;
import io.grpc.ChannelLogger;
import io.grpc.ConnectivityState;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancer.Helper;
import io.grpc.LoadBalancer.PickResult;
import io.grpc.LoadBalancer.PickSubchannelArgs;
import io.grpc.LoadBalancer.ResolvedAddresses;
import io.grpc.LoadBalancer.Subchannel;
import io.grpc.LoadBalancer.SubchannelPicker;
import io.grpc.LoadBalancerProvider;
import io.grpc.SynchronizationContext;
import io.grpc.internal.FakeClock;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.xds.XdsComms.AdsStreamCallback;
import io.grpc.xds.XdsLbState.LbEndpoint;
import io.grpc.xds.XdsLbState.Locality;
import io.grpc.xds.XdsLbState.LocalityInfo;
import io.grpc.xds.XdsLbState.LocalityState;
import io.grpc.xds.XdsLbState.SubchannelStore;
import io.grpc.xds.XdsLbState.SubchannelStoreImpl;
import io.grpc.xds.XdsLbState.WrrAlgorithm;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * Unit tests for {@link XdsLbState}.
 */
@RunWith(JUnit4.class)
public class XdsLbStateTest {
  @Rule
  public final GrpcCleanupRule cleanupRule = new GrpcCleanupRule();
  @Mock
  private Helper helper;
  @Mock
  private SubchannelPool subchannelPool;
  @Mock
  private AdsStreamCallback adsStreamCallback;
  @Mock
  private PickSubchannelArgs pickSubchannelArgs;
  @Captor
  private ArgumentCaptor<SubchannelPicker> subchannelPickerCaptor;
  @Captor
  private ArgumentCaptor<ResolvedAddresses> resolvedAddressesCaptor;

  private static final class FakeWrrAlgorith implements WrrAlgorithm {
    int nextIndex;

    void setNextIndex(int i) {
      nextIndex = i;
    }

    @Override
    public Locality pickLocality(List<LocalityState> wrrList) {
      return wrrList.get(nextIndex).locality;
    }
  }

  private FakeWrrAlgorith wrrAlgorithm = new FakeWrrAlgorith();

  private final Map<String, LoadBalancer> loadBalancers = new HashMap<>();
  private final Map<String, Helper> childHelpers = new HashMap<>();

  private final LoadBalancerProvider childLbProvider = new LoadBalancerProvider() {
    @Override
    public boolean isAvailable() {
      return true;
    }

    @Override
    public int getPriority() {
      return 5;
    }

    @Override
    public String getPolicyName() {
      return "child_policy";
    }

    @Override
    public LoadBalancer newLoadBalancer(Helper helper) {
      if (loadBalancers.containsKey(helper.getAuthority())) {
        return loadBalancers.get(helper.getAuthority());
      }
      LoadBalancer loadBalancer = mock(LoadBalancer.class);
      loadBalancers.put(helper.getAuthority(), loadBalancer);
      childHelpers.put(helper.getAuthority(), helper);
      return loadBalancer;
    }
  };

  private XdsLbState xdsLbState;
  private SubchannelStore subchannelStore;

  private final FakeClock fakeClock = new FakeClock();

  private final SynchronizationContext syncContext = new SynchronizationContext(
      new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          throw new AssertionError(e);
        }
      });

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    doReturn(syncContext).when(helper).getSynchronizationContext();
    doReturn(fakeClock.getScheduledExecutorService()).when(helper).getScheduledExecutorService();
    doReturn("fake_authority").when(helper).getAuthority();
    doReturn(mock(ChannelLogger.class)).when(helper).getChannelLogger();


    subchannelStore = new SubchannelStoreImpl(helper, subchannelPool, wrrAlgorithm);
    xdsLbState = new XdsLbState(
        "fake_balancer_name", null, null, helper, subchannelStore, adsStreamCallback);
  }

  @Test
  public void shutdownAndReleaseXdsCommsDoesShutdown() {
    XdsLbState xdsLbState = mock(XdsLbState.class);
    xdsLbState.shutdownAndReleaseXdsComms();
    verify(xdsLbState).shutdown();
  }

  @Test
  public void handleSubchannelState() {
    subchannelStore.updateLoadBalancerProvider(childLbProvider);
    assertThat(loadBalancers).isEmpty();

    Locality locality1 = new Locality("r1", "z1", "sz1");
    EquivalentAddressGroup eag11 = new EquivalentAddressGroup(new InetSocketAddress("addr11", 11));
    EquivalentAddressGroup eag12 = new EquivalentAddressGroup(new InetSocketAddress("addr12", 12));

    LbEndpoint lbEndpoint11 = new LbEndpoint(eag11, 11, HealthStatus.HEALTHY);
    LbEndpoint lbEndpoint12 = new LbEndpoint(eag12, 12, HealthStatus.HEALTHY);
    LocalityInfo localityInfo1 = new LocalityInfo(ImmutableList.of(lbEndpoint11, lbEndpoint12), 1);


    Locality locality2 = new Locality("r2", "z2", "sz2");
    EquivalentAddressGroup eag21 = new EquivalentAddressGroup(new InetSocketAddress("addr21", 21));
    EquivalentAddressGroup eag22 = new EquivalentAddressGroup(new InetSocketAddress("addr22", 22));

    LbEndpoint lbEndpoint21 = new LbEndpoint(eag21, 21, HealthStatus.HEALTHY);
    LbEndpoint lbEndpoint22 = new LbEndpoint(eag22, 22, HealthStatus.HEALTHY);
    LocalityInfo localityInfo2 = new LocalityInfo(ImmutableList.of(lbEndpoint21, lbEndpoint22), 2);

    Map<Locality, LocalityInfo> localityInfoMap = new LinkedHashMap<>();
    localityInfoMap.put(locality1, localityInfo1);
    localityInfoMap.put(locality2, localityInfo2);

    verify(helper, never()).updateBalancingState(
        any(ConnectivityState.class), any(SubchannelPicker.class));

    subchannelStore.updateLocalityStore(localityInfoMap);

    assertThat(loadBalancers).hasSize(2);
    assertThat(loadBalancers.keySet()).containsExactly("sz1", "sz2");
    assertThat(childHelpers).hasSize(2);
    assertThat(childHelpers.keySet()).containsExactly("sz1", "sz2");

    verify(loadBalancers.get("sz1")).handleResolvedAddresses(resolvedAddressesCaptor.capture());
    assertThat(resolvedAddressesCaptor.getValue().getServers())
        .containsExactly(eag11, eag12).inOrder();
    verify(loadBalancers.get("sz2")).handleResolvedAddresses(resolvedAddressesCaptor.capture());
    assertThat(resolvedAddressesCaptor.getValue().getServers())
        .containsExactly(eag21, eag22).inOrder();
    verify(helper, never()).updateBalancingState(
        any(ConnectivityState.class), any(SubchannelPicker.class));

    SubchannelPicker childPicker1 = mock(SubchannelPicker.class);
    PickResult pickResult1 = PickResult.withSubchannel(mock(Subchannel.class));
    doReturn(pickResult1).when(childPicker1).pickSubchannel(any(PickSubchannelArgs.class));
    childHelpers.get("sz1").updateBalancingState(READY, childPicker1);
    verify(helper).updateBalancingState(eq(READY), subchannelPickerCaptor.capture());

    wrrAlgorithm.setNextIndex(1);
    assertThat(subchannelPickerCaptor.getValue().pickSubchannel(pickSubchannelArgs)).isSameAs(
        PickResult.withNoResult());

    wrrAlgorithm.setNextIndex(0);
    assertThat(subchannelPickerCaptor.getValue().pickSubchannel(pickSubchannelArgs))
        .isSameAs(pickResult1);

    wrrAlgorithm.setNextIndex(1);
    assertThat(subchannelPickerCaptor.getValue().pickSubchannel(pickSubchannelArgs)).isSameAs(
        PickResult.withNoResult());

    SubchannelPicker childPicker2 = mock(SubchannelPicker.class);
    PickResult pickResult2 = PickResult.withSubchannel(mock(Subchannel.class));
    doReturn(pickResult2).when(childPicker2).pickSubchannel(any(PickSubchannelArgs.class));
    childHelpers.get("sz2").updateBalancingState(CONNECTING, childPicker2);
    verify(helper, times(2)).updateBalancingState(eq(READY), subchannelPickerCaptor.capture());

    assertThat(subchannelPickerCaptor.getValue().pickSubchannel(pickSubchannelArgs))
        .isSameAs(pickResult2);
  }
}
