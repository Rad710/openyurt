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
	ips  map[string]string // "ns/name" -> ip
	err  error
	hits int
}

func (f *fakeSource) PodIP(_ context.Context, namespace, name string) (string, bool, error) {
	f.hits++
	if f.err != nil {
		return "", false, f.err
	}
	ip, ok := f.ips[namespace+"/"+name]
	return ip, ok, nil
}

func (f *fakeSource) Close() error { return nil }

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
	if err := f.SetNodeName(nodeName); err != nil {
		t.Fatalf("SetNodeName: %v", err)
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
