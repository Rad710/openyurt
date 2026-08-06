/*
Copyright 2026 The OpenYurt Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package livepodip

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/openyurtio/openyurt/pkg/yurthub/filter"
	"github.com/openyurtio/openyurt/pkg/yurthub/filter/base"
	"github.com/openyurtio/openyurt/pkg/yurthub/filter/servicetopology"
	"github.com/openyurtio/openyurt/pkg/yurthub/healthchecker"
	fakehealthchecker "github.com/openyurtio/openyurt/pkg/yurthub/healthchecker/fake"
	"github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/cri"
)

const nodeName = "edge-1"

// fakeSource is a cri.Source backed by a fixed map.
type fakeSource struct {
	mu   sync.Mutex
	ips  map[string]string // "ns/name" -> ip
	err  error
	hits int

	// changesRemaining is how many further Refresh calls report a change.
	// Tests hand changes out ONE at a time and only once every waiter is
	// registered, so that a broadcast can be attributed to a specific change.
	// A source that reported "changed" on every call would let an
	// implementation that wakes exactly ONE waiter per broadcast satisfy N
	// waiters from N broadcasts and pass a test meant to rule that out.
	changesRemaining int
	refreshes        int
}

func (f *fakeSource) PodIP(_ context.Context, namespace, name string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits++
	if f.err != nil {
		return "", false, f.err
	}
	ip, ok := f.ips[namespace+"/"+name]
	return ip, ok, nil
}

func (f *fakeSource) Refresh(_ context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refreshes++
	if f.err != nil {
		return false, f.err
	}
	if f.changesRemaining > 0 {
		f.changesRemaining--
		return true, nil
	}
	return false, nil
}

// allowChanges releases n further address changes for the poller to find.
func (f *fakeSource) allowChanges(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.changesRemaining += n
}

func (f *fakeSource) Close() error { return nil }

func (f *fakeSource) refreshCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refreshes
}

// disconnected returns a checker reporting the cloud as unreachable, which is
// the only state in which this filter does anything.
func disconnected() healthchecker.Interface {
	u, _ := url.Parse("https://10.0.0.1:6443")
	return fakehealthchecker.NewFakeChecker(map[*url.URL]bool{u: false})
}

func connected() healthchecker.Interface {
	u, _ := url.Parse("https://10.0.0.1:6443")
	return fakehealthchecker.NewFakeChecker(map[*url.URL]bool{u: true})
}

func newFilter(t *testing.T, src cri.Source, checker healthchecker.Interface) *livePodIPFilter {
	t.Helper()
	f, err := NewLivePodIPFilter()
	if err != nil {
		t.Fatalf("NewLivePodIPFilter: %v", err)
	}
	if err := f.SetPodIPSource(src); err != nil {
		t.Fatalf("SetPodIPSource: %v", err)
	}
	if err := f.SetHealthChecker(checker); err != nil {
		t.Fatalf("SetHealthChecker: %v", err)
	}
	return f
}

// slice builds an EndpointSlice whose endpoints target pods by name.
func slice(namespace, name string, endpoints ...discovery.Endpoint) *discovery.EndpointSlice {
	return &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    map[string]string{discovery.LabelServiceName: name},
		},
		AddressType: discovery.AddressTypeIPv4,
		Endpoints:   endpoints,
	}
}

func podEndpoint(podNamespace, podName, address string) discovery.Endpoint {
	return discovery.Endpoint{
		Addresses: []string{address},
		TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: podNamespace, Name: podName},
	}
}

func addressesOf(obj runtime.Object) [][]string {
	eps, ok := obj.(*discovery.EndpointSlice)
	if !ok {
		return nil
	}
	out := make([][]string, 0, len(eps.Endpoints))
	for i := range eps.Endpoints {
		out = append(out, eps.Endpoints[i].Addresses)
	}
	return out
}

func TestRegisterAndName(t *testing.T) {
	filters := base.NewFilters([]string{})
	Register(filters)
	if !filters.Enabled(FilterName) {
		t.Errorf("expected %s to be registered and enabled", FilterName)
	}
	f, _ := NewLivePodIPFilter()
	if f.Name() != FilterName {
		t.Errorf("Name() = %q, want %q", f.Name(), FilterName)
	}
}

// TestRewritesStaleAddress is the core case: after a disconnected reboot the
// cached slice holds the pre-reboot address and the pod now has a new one.
func TestRewritesStaleAddress(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}}
	f := newFilter(t, src, disconnected())

	in := slice("default", "web", podEndpoint("default", "web-1", "10.244.1.8"))
	out := f.Filter(in, nil)

	if got := addressesOf(out); !reflect.DeepEqual(got, [][]string{{"10.244.1.11"}}) {
		t.Fatalf("addresses = %v, want [[10.244.1.11]]", got)
	}
}

// TestDoesNotMutateInput is the hard constraint from the task spec: the object
// handed to a filter comes from a shared cacher, so the original must be
// untouched even though we return a changed one.
func TestDoesNotMutateInput(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}}
	f := newFilter(t, src, disconnected())

	in := slice("default", "web", podEndpoint("default", "web-1", "10.244.1.8"))
	out := f.Filter(in, nil)

	if in.Endpoints[0].Addresses[0] != "10.244.1.8" {
		t.Errorf("input object was mutated in place: address is now %q", in.Endpoints[0].Addresses[0])
	}
	if out == runtime.Object(in) {
		t.Errorf("expected a copy to be returned when something changed, got the same pointer")
	}
}

// TestNoChangeReturnsOriginalPointer proves the lazy-copy optimisation: the
// common case (already correct) must not allocate a copy.
func TestNoChangeReturnsOriginalPointer(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}}
	f := newFilter(t, src, disconnected())

	in := slice("default", "web", podEndpoint("default", "web-1", "10.244.1.11"))
	out := f.Filter(in, nil)

	if out != runtime.Object(in) {
		t.Errorf("expected the original pointer back when nothing changed")
	}
}

// TestSkipsWhenCloudReachable — the real endpoint controller is authoritative
// while connected, and the lab confirmed a clean reconnect that must not break.
func TestSkipsWhenCloudReachable(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}}
	f := newFilter(t, src, connected())

	in := slice("default", "web", podEndpoint("default", "web-1", "10.244.1.8"))
	out := f.Filter(in, nil)

	if got := addressesOf(out); !reflect.DeepEqual(got, [][]string{{"10.244.1.8"}}) {
		t.Fatalf("addresses = %v, want the cached [[10.244.1.8]] left alone while connected", got)
	}
	if src.hits != 0 {
		t.Errorf("the runtime must not even be queried while the cloud is reachable, got %d calls", src.hits)
	}
}

// TestInactiveWithoutDependencies — fail closed. A nil source or nil checker
// must leave everything alone rather than guess.
func TestInactiveWithoutDependencies(t *testing.T) {
	in := slice("default", "web", podEndpoint("default", "web-1", "10.244.1.8"))

	t.Run("nil source", func(t *testing.T) {
		f, _ := NewLivePodIPFilter()
		_ = f.SetHealthChecker(disconnected())
		if out := f.Filter(in.DeepCopy(), nil); !reflect.DeepEqual(addressesOf(out), [][]string{{"10.244.1.8"}}) {
			t.Errorf("expected no change with a nil pod-IP source")
		}
	})

	t.Run("nil checker", func(t *testing.T) {
		f, _ := NewLivePodIPFilter()
		_ = f.SetPodIPSource(&fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}})
		if out := f.Filter(in.DeepCopy(), nil); !reflect.DeepEqual(addressesOf(out), [][]string{{"10.244.1.8"}}) {
			t.Errorf("expected no change with a nil health checker")
		}
	})
}

// TestUnresolvedEndpointsLeftAlone covers every reason PodIP says "no answer".
// The task spec is explicit: a stale address gives 502 and recovers, an emptied
// EndpointSlice gives 503 with no recovery path. Never drop, never blank.
func TestUnresolvedEndpointsLeftAlone(t *testing.T) {
	// web-2 is absent from the source: could be on another node, could have no
	// sandbox yet, could be hostNetwork (whose slice address is the node IP and
	// is already correct). The filter cannot tell them apart and must not try.
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}}
	f := newFilter(t, src, disconnected())

	in := slice("default", "web",
		podEndpoint("default", "web-1", "10.244.1.8"),
		podEndpoint("default", "web-2", "10.244.9.9"),
	)
	out := f.Filter(in, nil)

	want := [][]string{{"10.244.1.11"}, {"10.244.9.9"}}
	if got := addressesOf(out); !reflect.DeepEqual(got, want) {
		t.Fatalf("addresses = %v, want %v (web-2 untouched)", got, want)
	}
}

// TestRuntimeErrorLeavesWholeSliceAlone — if the runtime cannot be asked at
// all, do not half-rewrite a slice.
func TestRuntimeErrorLeavesWholeSliceAlone(t *testing.T) {
	src := &fakeSource{err: errors.New("runtime unavailable")}
	f := newFilter(t, src, disconnected())

	in := slice("default", "web", podEndpoint("default", "web-1", "10.244.1.8"))
	out := f.Filter(in, nil)

	if out != runtime.Object(in) {
		t.Errorf("expected the original object back when the runtime errored")
	}
}

func TestIgnoresNonPodAndMissingTargetRef(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}}
	f := newFilter(t, src, disconnected())

	in := slice("default", "web",
		discovery.Endpoint{Addresses: []string{"10.244.1.8"}}, // no targetRef at all
		discovery.Endpoint{ // targets a Node, not a Pod
			Addresses: []string{"10.244.1.7"},
			TargetRef: &corev1.ObjectReference{Kind: "Node", Namespace: "default", Name: "web-1"},
		},
	)
	out := f.Filter(in, nil)

	if out != runtime.Object(in) {
		t.Errorf("expected no change for endpoints without a Pod targetRef")
	}
}

// TestAddressFamilyMismatchLeftAlone: a slice carries a single address family.
// Writing the other family into it would produce an object the consumer cannot
// use. This is the bidirectional version of a bug found in the shell probe.
func TestAddressFamilyMismatchLeftAlone(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "fd00:10:244:1::11"}}
	f := newFilter(t, src, disconnected())

	in := slice("default", "web", podEndpoint("default", "web-1", "10.244.1.8"))
	in.AddressType = discovery.AddressTypeIPv4
	out := f.Filter(in, nil)

	if out != runtime.Object(in) {
		t.Errorf("expected an IPv6 live address to be ignored for an IPv4 slice")
	}
}

func TestFQDNSliceLeftAlone(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}}
	f := newFilter(t, src, disconnected())

	in := slice("default", "web", podEndpoint("default", "web-1", "web-1.example.com"))
	in.AddressType = discovery.AddressTypeFQDN
	out := f.Filter(in, nil)

	if out != runtime.Object(in) {
		t.Errorf("expected an FQDN-typed slice to be left alone")
	}
}

// TestTargetRefNamespaceFallback: targetRef.namespace is optional, so fall back
// to the slice's own namespace rather than looking up "/name".
func TestTargetRefNamespaceFallback(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"prod/web-1": "10.244.1.11"}}
	f := newFilter(t, src, disconnected())

	in := slice("prod", "web", discovery.Endpoint{
		Addresses: []string{"10.244.1.8"},
		TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: "web-1"}, // no namespace
	})
	out := f.Filter(in, nil)

	if got := addressesOf(out); !reflect.DeepEqual(got, [][]string{{"10.244.1.11"}}) {
		t.Fatalf("addresses = %v, want [[10.244.1.11]] via the slice's namespace", got)
	}
}

func TestNonEndpointSliceObjectPassesThrough(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}}
	f := newFilter(t, src, disconnected())

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "web-1"}}
	if out := f.Filter(pod, nil); out != runtime.Object(pod) {
		t.Errorf("a non-EndpointSlice object must pass through untouched")
	}
}

// newServiceTopologyFilter builds a real servicetopology filter configured for
// node-local topology, so composition is tested against the actual filter
// rather than a stand-in.
func newServiceTopologyFilter(t *testing.T) filter.ObjectFilter {
	t.Helper()
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "web",
			Annotations: map[string]string{servicetopology.AnnotationServiceTopologyKey: servicetopology.AnnotationServiceTopologyValueNode},
		},
	}
	client := fake.NewSimpleClientset(svc)
	factory := informers.NewSharedInformerFactory(client, 24*time.Hour)

	stf, err := servicetopology.NewServiceTopologyFilter()
	if err != nil {
		t.Fatalf("NewServiceTopologyFilter: %v", err)
	}
	type wantsFactory interface {
		SetSharedInformerFactory(informers.SharedInformerFactory) error
	}
	type wantsNodeName interface{ SetNodeName(string) error }
	if err := stf.(wantsFactory).SetSharedInformerFactory(factory); err != nil {
		t.Fatalf("SetSharedInformerFactory: %v", err)
	}
	if err := stf.(wantsNodeName).SetNodeName(nodeName); err != nil {
		t.Fatalf("SetNodeName: %v", err)
	}

	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })
	factory.Start(stopCh)
	factory.WaitForCacheSync(stopCh)
	return stf
}

// TestComposesWithServiceTopologyInBothOrders is required by the task spec:
// filters for a request key come out of an unsorted set, so chain order is
// nondeterministic and both orders must produce the same correct result.
//
// servicetopology (node topology) keeps only endpoints whose nodeName is this
// node; livepodip refreshes their addresses. Composed, the answer must be
// "the local endpoint, with its live address" either way round.
func TestComposesWithServiceTopologyInBothOrders(t *testing.T) {
	build := func() (*discovery.EndpointSlice, *fakeSource) {
		local := podEndpoint("default", "web-1", "10.244.1.8")
		local.NodeName = ptr(nodeName)
		remote := podEndpoint("default", "web-2", "10.244.2.5")
		remote.NodeName = ptr("other-node")
		return slice("default", "web", local, remote),
			&fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}}
	}

	want := [][]string{{"10.244.1.11"}}

	t.Run("servicetopology first", func(t *testing.T) {
		in, src := build()
		stf := newServiceTopologyFilter(t)
		lpf := newFilter(t, src, disconnected())

		out := lpf.Filter(stf.Filter(in, nil), nil)
		if got := addressesOf(out); !reflect.DeepEqual(got, want) {
			t.Fatalf("addresses = %v, want %v", got, want)
		}
	})

	t.Run("livepodip first", func(t *testing.T) {
		in, src := build()
		stf := newServiceTopologyFilter(t)
		lpf := newFilter(t, src, disconnected())

		out := stf.Filter(lpf.Filter(in, nil), nil)
		if got := addressesOf(out); !reflect.DeepEqual(got, want) {
			t.Fatalf("addresses = %v, want %v", got, want)
		}
	})
}

func ptr(s string) *string { return &s }

// --- invalidation: telling open watches that served addresses are now wrong ---

func newFilterWithPoll(t *testing.T, src cri.Source, checker healthchecker.Interface, poll time.Duration) *livePodIPFilter {
	t.Helper()
	f := newFilter(t, src, checker)
	f.pollInterval = poll
	return f
}

// The core of the propagation fix: while disconnected, a change in pod addresses
// must close the invalidation channel so open watches end and clients re-LIST.
func TestInvalidatesWhenAddressesChangeWhileOffline(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}, changesRemaining: 1}
	f := newFilterWithPoll(t, src, disconnected(), 10*time.Millisecond)

	stop := make(chan struct{})
	defer close(stop)
	ch := f.Invalidated(stop)

	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("invalidation never fired despite the address set changing while offline")
	}
}

// Every open watch must be told, and told by the SAME change. There is one
// waiter per open watch (kube-proxy, coredns, the ingress controller), and a
// broadcast that reaches only one of them leaves the others serving dead
// addresses for the rest of the outage.
//
// The oracle matters as much as the assertion here. Every waiter is registered
// BEFORE the single change is released, and only one change is ever released —
// otherwise an implementation that wakes exactly one waiter per broadcast (a
// buffered channel and a non-blocking send, which is what the struct comment
// warns against) satisfies five waiters from five broadcasts and passes.
func TestInvalidationWakesEveryWaiter(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}, changesRemaining: 0}
	f := newFilterWithPoll(t, src, disconnected(), 10*time.Millisecond)

	stop := make(chan struct{})
	defer close(stop)

	const waiters = 5
	chans := make([]<-chan struct{}, 0, waiters)
	for i := 0; i < waiters; i++ {
		chans = append(chans, f.Invalidated(stop))
	}
	// Let the poller run a few times with nothing to report, so registration is
	// definitely complete before the one change appears.
	if !waitForCount(func() bool { return src.refreshCount() >= 2 }, 2*time.Second) {
		t.Fatal("poller never ran")
	}

	src.allowChanges(1)

	var wg sync.WaitGroup
	woken := make([]bool, waiters)
	for i, ch := range chans {
		wg.Add(1)
		go func(i int, ch <-chan struct{}) {
			defer wg.Done()
			select {
			case <-ch:
				woken[i] = true
			case <-time.After(5 * time.Second):
			}
		}(i, ch)
	}
	wg.Wait()

	for i, ok := range woken {
		if !ok {
			t.Errorf("waiter %d was not woken by the single address change", i)
		}
	}
}

// While the cloud is reachable the real endpoint controller is authoritative and
// this filter rewrites nothing, so terminating watches would be pure churn.
func TestDoesNotInvalidateWhileCloudReachable(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}, changesRemaining: 1}
	f := newFilterWithPoll(t, src, connected(), 10*time.Millisecond)

	stop := make(chan struct{})
	defer close(stop)
	ch := f.Invalidated(stop)

	select {
	case <-ch:
		t.Fatal("invalidated while the cloud was reachable")
	case <-time.After(300 * time.Millisecond):
	}
	if got := src.refreshCount(); got != 0 {
		t.Errorf("runtime was polled %d times while connected, want 0", got)
	}
}

// An unchanged address set must not invalidate: doing so on every poll would
// make clients re-LIST forever.
func TestDoesNotInvalidateWithoutChange(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}, changesRemaining: 0}
	f := newFilterWithPoll(t, src, disconnected(), 10*time.Millisecond)

	stop := make(chan struct{})
	defer close(stop)
	ch := f.Invalidated(stop)

	select {
	case <-ch:
		t.Fatal("invalidated with no address change — this would be a re-LIST hot loop")
	case <-time.After(300 * time.Millisecond):
	}
	if src.refreshCount() == 0 {
		t.Error("expected the runtime to be polled while disconnected")
	}
}

// A second change after the first must be observable, so a node whose
// addresses churn twice converges twice. Each round is driven by its own
// released change, not by another tick of an always-changed source — otherwise
// this only proves that close-and-replace does not panic.
func TestInvalidatesRepeatedly(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}, changesRemaining: 0}
	f := newFilterWithPoll(t, src, disconnected(), 10*time.Millisecond)

	stop := make(chan struct{})
	defer close(stop)

	for round := 1; round <= 2; round++ {
		ch := f.Invalidated(stop)
		mark := src.refreshCount()
		if !waitForCount(func() bool { return src.refreshCount() > mark }, 2*time.Second) {
			t.Fatalf("round %d: poller not running", round)
		}
		select {
		case <-ch:
			t.Fatalf("round %d: invalidated before any change was released", round)
		default:
		}

		src.allowChanges(1)
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatalf("round %d: invalidation did not fire for its own change", round)
		}
	}
}

// Nothing to detect without both dependencies, and no goroutine should be left
// polling a nil source.
func TestNoPollerWithoutDependencies(t *testing.T) {
	f, _ := NewLivePodIPFilter()
	_ = f.SetHealthChecker(disconnected())
	f.pollInterval = 10 * time.Millisecond

	stop := make(chan struct{})
	defer close(stop)
	ch := f.Invalidated(stop)

	select {
	case <-ch:
		t.Fatal("invalidated with a nil pod-IP source")
	case <-time.After(200 * time.Millisecond):
	}
}

// The poller must stop with its caller rather than outliving it.
// The poller must stop once the LAST watch goes away — not the first. An
// earlier version of this test closed a single watch and asserted the poller
// stopped, which passed while the implementation was wrong: the poller was
// bound to whichever watch registered first, so that watch ending disabled
// invalidation for every other watch on the node. See
// TestPollerSurvivesFirstWatchClosing.
func TestPollerStopsWhenTheLastWatchGoesAway(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}}
	f := newFilterWithPoll(t, src, disconnected(), 10*time.Millisecond)

	first, second := make(chan struct{}), make(chan struct{})
	f.Invalidated(first)
	f.Invalidated(second)

	if !waitForCount(func() bool { return src.refreshCount() > 0 }, 2*time.Second) {
		t.Fatal("poller never ran")
	}

	// One watch ending must NOT stop the poller: the other is still open.
	close(first)
	time.Sleep(100 * time.Millisecond)
	mark := src.refreshCount()
	if !waitForCount(func() bool { return src.refreshCount() > mark }, 2*time.Second) {
		t.Fatal("poller stopped when only the first of two watches ended")
	}

	// With the last one gone there is nobody to invalidate, so it must stop.
	close(second)
	time.Sleep(100 * time.Millisecond)
	settled := src.refreshCount()
	time.Sleep(300 * time.Millisecond)
	if got := src.refreshCount(); got != settled {
		t.Errorf("poller kept running after the last watch ended: %d -> %d", settled, got)
	}
}

// Regression: the filter is a process-lifetime singleton shared by every
// request, so a later watch must still be invalidated after an earlier one has
// come and gone. This failed before the poller was untied from the first
// caller's stop channel — the second waiter was never woken.
func TestPollerSurvivesFirstWatchClosing(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}, changesRemaining: 0}
	f := newFilterWithPoll(t, src, disconnected(), 10*time.Millisecond)

	first := make(chan struct{})
	ch1 := f.Invalidated(first)
	src.allowChanges(1)
	select {
	case <-ch1:
	case <-time.After(3 * time.Second):
		t.Fatal("first waiter never got a signal")
	}
	close(first)
	time.Sleep(200 * time.Millisecond)

	// A later watch — e.g. kube-proxy reconnecting — must still be invalidated
	// by a change of its own.
	second := make(chan struct{})
	defer close(second)
	ch2 := f.Invalidated(second)
	src.allowChanges(1)
	select {
	case <-ch2:
	case <-time.After(3 * time.Second):
		t.Fatal("second waiter never invalidated: the poller did not outlive the first watch")
	}
}

// A watch that registers after the poller is already running must be able to
// stop it when it turns out to be the last one left.
func TestPollerRestartsForALaterWatch(t *testing.T) {
	src := &fakeSource{ips: map[string]string{"default/web-1": "10.244.1.11"}}
	f := newFilterWithPoll(t, src, disconnected(), 10*time.Millisecond)

	first := make(chan struct{})
	f.Invalidated(first)
	if !waitForCount(func() bool { return src.refreshCount() > 0 }, 2*time.Second) {
		t.Fatal("poller never ran")
	}
	close(first)
	time.Sleep(200 * time.Millisecond)
	idle := src.refreshCount()

	second := make(chan struct{})
	defer close(second)
	f.Invalidated(second)
	if !waitForCount(func() bool { return src.refreshCount() > idle }, 2*time.Second) {
		t.Fatal("poller did not restart for a watch arriving after the previous one ended")
	}
}

func waitForCount(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}
