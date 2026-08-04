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
	"testing"
	"time"

	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
)

// passthroughFilter is an ObjectFilter that changes nothing and, optionally,
// can announce that its output has changed.
type passthroughFilter struct {
	name        string
	invalidated chan struct{} // nil means it does not implement WatchInvalidator
	gotStop     chan struct{} // closed with the stop channel it was handed
}

func (p *passthroughFilter) Name() string { return p.name }
func (p *passthroughFilter) SupportedResourceAndVerbs() map[string]sets.Set[string] {
	return map[string]sets.Set[string]{}
}
func (p *passthroughFilter) Filter(obj runtime.Object, _ <-chan struct{}) runtime.Object {
	return obj
}

type invalidatingFilter struct{ *passthroughFilter }

func (i *invalidatingFilter) Invalidated(stop <-chan struct{}) <-chan struct{} {
	if i.gotStop != nil {
		go func() {
			<-stop
			close(i.gotStop)
		}()
	}
	return i.invalidated
}

// The load-bearing behaviour: when the filter says its output changed, the watch
// must end with a watch.Error carrying a 410, because reflector_contract_test.go
// proves that is the only thing that makes client-go re-LIST. Anything else —
// a clean close, a Bookmark, a Modified — leaves the client on stale addresses.
func TestFilterWatchEmits410OnInvalidation(t *testing.T) {
	source := watch.NewFake()
	inv := make(chan struct{})
	f := &invalidatingFilter{&passthroughFilter{name: "test", invalidated: inv}}

	w := newFilterWatch(source, f)
	defer w.Stop()

	close(inv)

	select {
	case ev, ok := <-w.ResultChan():
		if !ok {
			t.Fatal("watch closed with no event; the client would re-WATCH, not re-LIST")
		}
		if ev.Type != watch.Error {
			t.Fatalf("event type = %v, want watch.Error", ev.Type)
		}
		status, ok := ev.Object.(*metav1.Status)
		if !ok {
			t.Fatalf("event object is %T, want *metav1.Status", ev.Object)
		}
		if status.Code != 410 {
			t.Errorf("status code = %d, want 410", status.Code)
		}
		if status.Reason != metav1.StatusReasonExpired {
			t.Errorf("reason = %q, want %q", status.Reason, metav1.StatusReasonExpired)
		}
		// client-go's handleAnyWatch runs the event through
		// apierrors.FromObject and relies on this classifying as "expired".
		if err := errors.FromObject(ev.Object); !errors.IsResourceExpired(err) && !errors.IsGone(err) {
			t.Errorf("client-go would not classify this as expired/gone: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event emitted after invalidation")
	}
}

// Exactly one error, then the channel closes: a second event on a watch the
// reflector has already abandoned would block the sender forever.
func TestFilterWatchClosesAfterInvalidation(t *testing.T) {
	source := watch.NewFake()
	inv := make(chan struct{})
	f := &invalidatingFilter{&passthroughFilter{name: "test", invalidated: inv}}

	w := newFilterWatch(source, f)
	defer w.Stop()

	close(inv)

	if ev, ok := <-w.ResultChan(); !ok || ev.Type != watch.Error {
		t.Fatalf("first event = %v ok=%v, want a watch.Error", ev.Type, ok)
	}
	select {
	case ev, ok := <-w.ResultChan():
		if ok {
			t.Fatalf("expected the channel to close after the error, got %v", ev.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("channel neither closed nor delivered after the error")
	}
}

// An invalidation that never fires must not disturb normal delivery.
func TestFilterWatchStillForwardsEventsWhenNotInvalidated(t *testing.T) {
	source := watch.NewFake()
	f := &invalidatingFilter{&passthroughFilter{name: "test", invalidated: make(chan struct{})}}

	w := newFilterWatch(source, f)
	defer w.Stop()

	go source.Add(&discovery.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "es", Namespace: "default"}})

	select {
	case ev := <-w.ResultChan():
		if ev.Type != watch.Added {
			t.Fatalf("event type = %v, want Added", ev.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a normal event was not forwarded")
	}
}

// A filter that does not implement WatchInvalidator must behave exactly as
// before — most filters in the tree do not.
func TestFilterWatchUnaffectedByNonInvalidatingFilter(t *testing.T) {
	source := watch.NewFake()
	w := newFilterWatch(source, &passthroughFilter{name: "plain"})
	defer w.Stop()

	go source.Add(&discovery.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "es", Namespace: "default"}})

	select {
	case ev := <-w.ResultChan():
		if ev.Type != watch.Added {
			t.Fatalf("event type = %v, want Added", ev.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a normal event was not forwarded")
	}
}

// The stop channel handed to the filter must be the watch's own, so the filter
// can release whatever bookkeeping it keeps per watch when the watch ends.
func TestFilterWatchReleasesTheFilterOnStop(t *testing.T) {
	source := watch.NewFake()
	released := make(chan struct{})
	f := &invalidatingFilter{&passthroughFilter{
		name: "test", invalidated: make(chan struct{}), gotStop: released,
	}}

	w := newFilterWatch(source, f)
	w.Stop()

	select {
	case <-released:
	case <-time.After(3 * time.Second):
		t.Fatal("the filter's stop channel was never closed; per-watch state would leak")
	}
}
