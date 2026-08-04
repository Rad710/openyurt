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

package multiplexer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

// These two tests pin behaviour of k8s.io/client-go that yurthub's offline
// serve-time filtering DEPENDS ON. They are not testing our code; they are
// testing an assumption about someone else's, deliberately, because getting it
// wrong has already cost one discarded implementation.
//
// Background: while the cloud is unreachable the multiplexer's cacher is fed
// from an unchanging disk cache, so it emits no watch events. A filter that
// corrects objects on the way out (livepodip, rewriting EndpointSlice addresses
// to the pod addresses actually live on this node) therefore only takes effect
// on a LIST. Making a client re-LIST is the whole mechanism, so it matters
// enormously *what* makes a reflector re-LIST.
//
// The answer, pinned below: a watch.Error does; a clean watch close does not.

// countingListerWatcher counts LIST calls and hands each new watch to the test.
type countingListerWatcher struct {
	mu       sync.Mutex
	lists    int
	watchers chan *watch.FakeWatcher
}

func (l *countingListerWatcher) List(_ metav1.ListOptions) (runtime.Object, error) {
	l.mu.Lock()
	l.lists++
	l.mu.Unlock()
	return &discovery.EndpointSliceList{
		ListMeta: metav1.ListMeta{ResourceVersion: "1"},
	}, nil
}

func (l *countingListerWatcher) Watch(_ metav1.ListOptions) (watch.Interface, error) {
	fw := watch.NewFake()
	select {
	case l.watchers <- fw:
	default: // test is not waiting for this one
	}
	return fw, nil
}

func (l *countingListerWatcher) listCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lists
}

// startReflector runs a real cache.Reflector against lw and returns once the
// initial LIST has happened.
func startReflector(t *testing.T, lw *countingListerWatcher) {
	t.Helper()
	r := cache.NewReflector(lw, &discovery.EndpointSlice{}, cache.NewStore(cache.MetaNamespaceKeyFunc), 0)
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })
	go r.Run(stopCh)

	if !waitFor(t, 5*time.Second, func() bool { return lw.listCount() >= 1 }) {
		t.Fatalf("reflector never performed its initial LIST")
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

func nextWatcher(t *testing.T, lw *countingListerWatcher) *watch.FakeWatcher {
	t.Helper()
	select {
	case fw := <-lw.watchers:
		return fw
	case <-time.After(5 * time.Second):
		t.Fatalf("reflector never established a watch")
		return nil
	}
}

// TestWatchErrorMakesReflectorReList is THE assumption the propagation fix rests
// on: injecting a watch.Error into a client's watch makes it perform a fresh
// LIST, which is what runs the serve-time filter again.
//
// A pod restart works today for exactly this reason — a restart is just a forced
// LIST. This proves yurthub can trigger the same thing without one.
func TestWatchErrorMakesReflectorReList(t *testing.T) {
	lw := &countingListerWatcher{watchers: make(chan *watch.FakeWatcher, 4)}
	startReflector(t, lw)
	before := lw.listCount()

	fw := nextWatcher(t, lw)
	// An expired/410 Status is what a server sends to say "your view is
	// unusable, start over". This is the event the fix will inject.
	fw.Error(&metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    410,
		Reason:  metav1.StatusReasonExpired,
		Message: "too old resource version",
	})

	if !waitFor(t, 10*time.Second, func() bool { return lw.listCount() > before }) {
		t.Fatalf("watch.Error did NOT cause a re-LIST (still %d) — the propagation fix cannot work this way", lw.listCount())
	}
}

// TestCleanWatchCloseDoesNotReList pins the behaviour that invalidated an
// earlier attempt at this fix (capping the watch timeout so the watch would end
// sooner). A watch that ends WITHOUT an error does not cause a re-LIST: the
// reflector just re-establishes a watch from its last resourceVersion, and
// offline that yields no events, so the client keeps its stale view forever.
//
// The watch is held open for over a second on purpose: closing sooner with no
// events produces client-go's VeryShortWatchError, which IS an error and would
// re-LIST, masking the behaviour under test.
func TestCleanWatchCloseDoesNotReList(t *testing.T) {
	lw := &countingListerWatcher{watchers: make(chan *watch.FakeWatcher, 4)}
	startReflector(t, lw)
	before := lw.listCount()

	fw := nextWatcher(t, lw)
	time.Sleep(1200 * time.Millisecond) // avoid VeryShortWatchError
	fw.Stop()                           // clean close, no error, no events

	// Give the reflector ample opportunity to re-LIST if it were going to.
	time.Sleep(2 * time.Second)

	if got := lw.listCount(); got != before {
		t.Fatalf("clean watch close caused %d extra LIST(s); this test's premise (and the reason the watch-timeout approach was abandoned) is wrong", got-before)
	}
}

// TestReListReachesEventHandlersDespiteUnchangedResourceVersion closes the last
// gap between "the client re-LISTs" and "the consumer reprograms".
//
// It is not obvious that it does. The livepodip filter rewrites addresses but
// leaves metadata.resourceVersion alone — it has no authority to invent one —
// so the re-LIST returns an object that is DIFFERENT but carries the SAME
// resourceVersion. client-go's sharedIndexInformer.OnUpdate
// (shared_informer.go:744-761) sets isSync = (new.RV == old.RV) and
// sharedProcessor.distribute then delivers sync notifications only to listeners
// that are currently resyncing. Read alone, that says a consumer's handler is
// skipped and kube-proxy never reprograms iptables — which would make the whole
// invalidation path useless in exactly the way capping the watch timeout was.
//
// It is not skipped, for two independent reasons, and this test pins both:
//   - addListener seeds p.listeners[listener] = true, and that only flips after
//     the first resync tick. After a disconnected reboot every consumer is
//     freshly started, so it is still true during the window that matters.
//   - once ticks do flip it, resync re-delivers from the store, which the
//     re-LIST has already corrected.
//
// The hardware drill could not have caught this: it restarted the consumers,
// which builds a fresh informer with an empty store, so every object arrives as
// an Add and the sync path is never exercised.
func TestReListReachesEventHandlersDespiteUnchangedResourceVersion(t *testing.T) {
	for _, tc := range []struct {
		name   string
		resync time.Duration
	}{
		{"no resync", 0},
		{"long resync, no tick yet (a freshly rebooted consumer)", 15 * time.Minute},
		{"short resync, several ticks already elapsed", time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lw := &rewritingListerWatcher{watchers: make(chan *watch.FakeWatcher, 4)}
			lw.address.Store(deadAddress)

			informer := cache.NewSharedIndexInformer(
				lw, &discovery.EndpointSlice{}, tc.resync, cache.Indexers{})

			var seen atomic.Value
			seen.Store("")
			record := func(o interface{}) {
				seen.Store(o.(*discovery.EndpointSlice).Endpoints[0].Addresses[0])
			}
			if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc:    record,
				UpdateFunc: func(_, n interface{}) { record(n) },
			}); err != nil {
				t.Fatalf("AddEventHandler: %v", err)
			}

			stopCh := make(chan struct{})
			t.Cleanup(func() { close(stopCh) })
			go informer.Run(stopCh)
			if !cache.WaitForCacheSync(stopCh, informer.HasSynced) {
				t.Fatal("informer never synced")
			}
			if got := seen.Load().(string); got != deadAddress {
				t.Fatalf("initial LIST delivered %q, want the stale %q", got, deadAddress)
			}
			if tc.resync > 0 && tc.resync < time.Minute {
				time.Sleep(3 * tc.resync) // let resync ticks flip isSyncing to false
			}

			// The filter now resolves the live address. Same object, same RV.
			lw.address.Store(liveAddress)

			// yurthub injects the 410, exactly as filterWatch.sendExpired does.
			select {
			case fw := <-lw.watchers:
				fw.Error(&metav1.Status{
					Status: metav1.StatusFailure, Code: 410,
					Reason: metav1.StatusReasonExpired, Message: "corrected while disconnected",
				})
			case <-time.After(5 * time.Second):
				t.Fatal("reflector never established a watch to invalidate")
			}

			if !waitFor(t, 10*time.Second, func() bool { return seen.Load().(string) == liveAddress }) {
				t.Fatalf("handler still on %q: the consumer would keep dead addresses programmed", seen.Load())
			}
		})
	}
}

const (
	deadAddress = "10.0.0.22"
	liveAddress = "10.0.0.31"
)

// rewritingListerWatcher serves one EndpointSlice at a fixed resourceVersion
// whose address can change underneath it, modelling a serve-time filter.
type rewritingListerWatcher struct {
	address  atomic.Value // string
	watchers chan *watch.FakeWatcher
}

func (l *rewritingListerWatcher) List(_ metav1.ListOptions) (runtime.Object, error) {
	return &discovery.EndpointSliceList{
		ListMeta: metav1.ListMeta{ResourceVersion: "100"},
		Items: []discovery.EndpointSlice{{
			ObjectMeta: metav1.ObjectMeta{
				Name: "web", Namespace: "default", ResourceVersion: "42",
			},
			Endpoints: []discovery.Endpoint{{Addresses: []string{l.address.Load().(string)}}},
		}},
	}, nil
}

func (l *rewritingListerWatcher) Watch(_ metav1.ListOptions) (watch.Interface, error) {
	fw := watch.NewFake()
	select {
	case l.watchers <- fw:
	default:
	}
	return fw, nil
}
