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

// Package livepodip serves EndpointSlices with the pod addresses that are
// actually live on this node, while the cloud is unreachable.
//
// The problem it solves: after a reboot taken while disconnected, the CNI
// assigns every pod a new address, but EndpointSlices are written only by the
// control-plane endpoint controller, which is unreachable. yurthub replays the
// addresses it last saw — all of them dead — kube-proxy and CoreDNS program
// them, and the application returns 502 until an operator hand-edits JSON
// inside yurthub's cache and restarts three components.
//
// This filter substitutes for the missing controller at serve time only. It
// matches endpoints on pod IDENTITY (endpoints[].targetRef namespace/name) and
// never on address, which is what makes it safe against the subtler failure:
// host-local allocates round-robin from last_reserved_ip, so once the cursor
// laps the node's /24 a freed address is reissued to a DIFFERENT pod. A cache
// keyed on address would then silently misroute — wrong answers rather than no
// answer. Keyed on identity, that cannot happen.
//
// See docs/edge-autonomy/decisions/0002 for why this shape was chosen over
// pinning pod IPs with a CNI plugin or repairing the cache from outside.
package livepodip

import (
	"context"
	"sync"
	"time"

	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	utilnet "k8s.io/utils/net"

	"github.com/openyurtio/openyurt/pkg/yurthub/filter"
	"github.com/openyurtio/openyurt/pkg/yurthub/filter/base"
	"github.com/openyurtio/openyurt/pkg/yurthub/healthchecker"
	"github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/cri"
)

// FilterName is used to rewrite cached EndpointSlice addresses with the pod
// addresses currently live on this node, while the cloud is unreachable.
const FilterName = "livepodip"

// DefaultPollInterval is how often the container runtime is re-read while the
// cloud is unreachable, to notice that pod addresses have changed and that
// already-served EndpointSlices are therefore wrong.
//
// Nothing else refreshes the pod-IP source once clients have finished their
// initial LIST, which is precisely the state after a disconnected reboot. This
// bounds how long a client can hold a stale view; convergence follows within
// roughly this interval plus the client's re-LIST.
const DefaultPollInterval = 5 * time.Second

// Register registers a filter
func Register(filters *base.Filters) {
	filters.Register(FilterName, func() (filter.ObjectFilter, error) {
		return NewLivePodIPFilter()
	})
}

func NewLivePodIPFilter() (*livePodIPFilter, error) {
	return &livePodIPFilter{
		invalidated:  make(chan struct{}),
		pollInterval: DefaultPollInterval,
	}, nil
}

type livePodIPFilter struct {
	// Both are injected after construction and may legitimately be nil: a
	// non-edge working mode has no health checker, and a node whose container
	// runtime endpoint could not be parsed has no pod-IP source. Either being
	// nil disables rewriting entirely — see Filter.
	checker    healthchecker.Interface
	podIPs     cri.Source
	nodeName   string
	warnedNoIP bool

	pollInterval time.Duration

	// The poller's lifetime is tied to the SET of open watches, not to any one
	// of them. This filter is a process-lifetime singleton — the filter manager
	// builds one instance and hands it to every request (filter/manager:
	// NewFromFilters is called once) — whereas each filterWatch passes its own
	// stop channel. Binding the poller to whichever watch happened to call
	// first would let that watch ending kill invalidation for every watch after
	// it, silently, which is the failure shape this whole feature exists to
	// remove.
	pollMu       sync.Mutex
	pollWatchers int
	pollStop     chan struct{}

	// invalidated is closed-and-replaced to broadcast. A single buffered channel
	// would wake only ONE waiter, and there is one waiter per open watch
	// (kube-proxy, coredns, the ingress controller...) — every one of them needs
	// to be told, or the ones that miss out keep serving stale addresses.
	invalidatedMu sync.Mutex
	invalidated   chan struct{}
}

// Invalidated implements filter.WatchInvalidator.
func (f *livePodIPFilter) Invalidated(stop <-chan struct{}) <-chan struct{} {
	// Take the current channel BEFORE registering, so a broadcast racing with
	// this call closes the channel we are about to hand back rather than the
	// one that replaces it. Handing back the replacement would lose the signal
	// for this watch entirely.
	f.invalidatedMu.Lock()
	ch := f.invalidated
	f.invalidatedMu.Unlock()

	// Registering here rather than polling from construction means the runtime
	// is only read on nodes where something actually watches EndpointSlices.
	f.addWatcher(stop)
	return ch
}

func (f *livePodIPFilter) broadcastInvalidated() {
	f.invalidatedMu.Lock()
	defer f.invalidatedMu.Unlock()
	close(f.invalidated)
	f.invalidated = make(chan struct{})
}

// addWatcher records one open watch and starts the poller if it is the first.
func (f *livePodIPFilter) addWatcher(stop <-chan struct{}) {
	if f.podIPs == nil || f.checker == nil {
		return // inactive: nothing to detect, so nothing to poll for
	}

	f.pollMu.Lock()
	f.pollWatchers++
	if f.pollWatchers == 1 {
		f.pollStop = make(chan struct{})
		go f.pollForAddressChanges(f.pollStop)
	}
	f.pollMu.Unlock()

	if stop == nil {
		// No way to learn when this watch ends, so never release it. That
		// leaves the poller running, which is the safe direction: over-polling
		// costs a ListPodSandbox every interval, whereas under-polling means
		// consumers keep serving dead addresses through an outage.
		return
	}

	// Exits as soon as this watch ends; bounded by the watch, not by the filter.
	go func() {
		<-stop
		f.removeWatcher()
	}()
}

// removeWatcher drops one open watch and stops the poller once none are left.
func (f *livePodIPFilter) removeWatcher() {
	f.pollMu.Lock()
	defer f.pollMu.Unlock()
	if f.pollWatchers == 0 {
		return // defensive: never let an extra release stop a live poller
	}
	f.pollWatchers--
	if f.pollWatchers == 0 && f.pollStop != nil {
		close(f.pollStop)
		f.pollStop = nil
	}
}

// pollForAddressChanges notices that the addresses this filter serves have
// changed and tells open watches, so their clients re-LIST and receive the
// corrected objects.
func (f *livePodIPFilter) pollForAddressChanges(stop <-chan struct{}) {
	ticker := time.NewTicker(f.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}

		// Only while disconnected. While the cloud is reachable the real
		// endpoint controller is authoritative, this filter does not rewrite
		// anything, and terminating watches would be pure churn.
		if f.checker.IsHealthy() {
			continue
		}

		changed, err := f.podIPs.Refresh(context.Background())
		if err != nil {
			klog.V(4).Infof("%s filter: could not refresh pod addresses: %v", FilterName, err)
			continue
		}
		if !changed {
			continue
		}
		klog.V(2).Infof("%s filter: pod addresses changed while disconnected, invalidating open watches so clients re-list", FilterName)
		f.broadcastInvalidated()
	}
}

func (f *livePodIPFilter) Name() string {
	return FilterName
}

// SetHealthChecker implements initializer.WantsHealthChecker. Note this is
// called AFTER construction, from filter.FilterFinder.SetHealthChecker, not
// through the usual initializer chain — the checker does not exist yet when
// filters are built. See pkg/yurthub/filter/interfaces.go.
func (f *livePodIPFilter) SetHealthChecker(checker healthchecker.Interface) error {
	f.checker = checker
	return nil
}

// SetPodIPSource implements initializer.WantsPodIPSource.
func (f *livePodIPFilter) SetPodIPSource(source cri.Source) error {
	f.podIPs = source
	return nil
}

// SetNodeName implements initializer.WantsNodeName. Used only for logging —
// the pod-IP source is inherently node-local, so endpoints targeting pods on
// other nodes resolve to "no answer" and are left alone without needing to
// compare node names.
func (f *livePodIPFilter) SetNodeName(nodeName string) error {
	f.nodeName = nodeName
	return nil
}

func (f *livePodIPFilter) Filter(obj runtime.Object, stopCh <-chan struct{}) runtime.Object {
	switch v := obj.(type) {
	case *discovery.EndpointSlice:
		return f.rewriteAddresses(v)
	default:
		return obj
	}
}

func (f *livePodIPFilter) rewriteAddresses(eps *discovery.EndpointSlice) *discovery.EndpointSlice {
	// Fail closed on every "cannot tell" case. Rewriting while the cloud is
	// reachable would fight the real endpoint controller, which is
	// authoritative; the lab has already confirmed a clean reconnect over
	// hand-patched cache state and that path must not be broken.
	if f.podIPs == nil || f.checker == nil {
		if !f.warnedNoIP {
			klog.Warningf("%s filter is registered but inactive: podIPSource=%v healthChecker=%v",
				FilterName, f.podIPs != nil, f.checker != nil)
			f.warnedNoIP = true
		}
		return eps
	}
	if f.checker.IsHealthy() {
		return eps
	}

	// context.Background() rather than one derived from stopCh: cri.Source
	// bounds each runtime round trip with its own timeout and deliberately
	// does not honour per-caller cancellation, because singleflight collapses
	// concurrent callers into one shared refresh and one caller going away
	// must not abort it for the others. Deriving a cancellable context here
	// would also cost a goroutine per served object on a hot path.
	ctx := context.Background()

	// Copy lazily: the overwhelming majority of served slices need no change
	// (nothing local, or already correct), and this runs on every LIST and
	// every watch event.
	var out *discovery.EndpointSlice
	for i := range eps.Endpoints {
		ep := &eps.Endpoints[i]
		if ep.TargetRef == nil || ep.TargetRef.Kind != "Pod" || ep.TargetRef.Name == "" {
			continue
		}
		namespace := ep.TargetRef.Namespace
		if namespace == "" {
			// targetRef.namespace is optional; an EndpointSlice's endpoints
			// are same-namespace as the slice in practice.
			namespace = eps.Namespace
		}

		liveIP, found, err := f.podIPs.PodIP(ctx, namespace, ep.TargetRef.Name)
		if err != nil {
			// The runtime could not be asked. Leave every address alone: a
			// stale address gives 502 and recovers, an emptied EndpointSlice
			// gives 503 with no recovery path.
			klog.V(4).Infof("%s filter: could not resolve %s/%s, leaving %s/%s untouched: %v",
				FilterName, namespace, ep.TargetRef.Name, eps.Namespace, eps.Name, err)
			return eps
		}
		if !found {
			// Three different states arrive here and none of them justifies a
			// change: the pod is on another node, it has no sandbox yet, or
			// the runtime reports no network status for it (hostNetwork pods,
			// whose address in the slice is the node IP and is CORRECT).
			//
			// decisions/0002 originally proposed serving conditions.ready=false
			// in this case. That is NOT implemented, deliberately: this filter
			// cannot distinguish "no sandbox yet" from "hostNetwork pod", so
			// marking not-ready would take a healthy hostNetwork endpoint out
			// of service. Leaving it alone is what the task's own plan asks
			// for ("prefer leave the address alone over write a wrong one").
			continue
		}
		if len(ep.Addresses) == 1 && ep.Addresses[0] == liveIP {
			continue // already correct, by far the common case
		}
		// A slice carries one address family (eps.AddressType). Never write an
		// address of the other family into it.
		if !matchesAddressType(liveIP, eps.AddressType) {
			klog.V(4).Infof("%s filter: live address %s for %s/%s does not match slice address type %s, leaving untouched",
				FilterName, liveIP, namespace, ep.TargetRef.Name, eps.AddressType)
			continue
		}

		if out == nil {
			out = eps.DeepCopy()
		}
		klog.V(2).Infof("%s filter: %s/%s addresses %v -> [%s] in endpointslice %s/%s",
			FilterName, namespace, ep.TargetRef.Name, ep.Addresses, liveIP, eps.Namespace, eps.Name)
		out.Endpoints[i].Addresses = []string{liveIP}
	}

	if out == nil {
		return eps
	}
	return out
}

func matchesAddressType(ip string, addressType discovery.AddressType) bool {
	switch addressType {
	case discovery.AddressTypeIPv4:
		return utilnet.IsIPv4String(ip)
	case discovery.AddressTypeIPv6:
		return utilnet.IsIPv6String(ip)
	default:
		// AddressTypeFQDN or anything unrecognised: not ours to rewrite.
		return false
	}
}
