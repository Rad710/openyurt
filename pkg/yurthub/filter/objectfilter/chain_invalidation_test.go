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

package objectfilter

import (
	"runtime"
	"testing"
	"time"

	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/openyurtio/openyurt/pkg/yurthub/filter"
)

// The watch layer only ever sees the composed chain, never its members, so a
// chain that did not forward invalidation would swallow the signal entirely and
// the whole propagation fix would be inert.

type plainFilter struct{ name string }

func (p *plainFilter) Name() string { return p.name }
func (p *plainFilter) SupportedResourceAndVerbs() map[string]sets.Set[string] {
	return map[string]sets.Set[string]{}
}
func (p *plainFilter) Filter(obj apiruntime.Object, _ <-chan struct{}) apiruntime.Object {
	return obj
}

type invalidatingFilter struct {
	plainFilter
	ch chan struct{}
}

func (i *invalidatingFilter) Invalidated(_ <-chan struct{}) <-chan struct{} { return i.ch }

func newInvalidating(name string) *invalidatingFilter {
	return &invalidatingFilter{plainFilter{name}, make(chan struct{})}
}

func mustNotFire(t *testing.T, ch <-chan struct{}, why string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(why)
	case <-time.After(200 * time.Millisecond):
	}
}

func mustFire(t *testing.T, ch <-chan struct{}, why string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal(why)
	}
}

// No member can invalidate: the channel must simply never fire, and must not be
// nil-in-disguise or already closed.
func TestChainWithNoInvalidatingMember(t *testing.T) {
	chain := CreateFilterChain([]filter.ObjectFilter{&plainFilter{"a"}, &plainFilter{"b"}})
	wi, ok := chain.(filter.WatchInvalidator)
	if !ok {
		t.Fatal("a chain must always implement WatchInvalidator")
	}
	stop := make(chan struct{})
	defer close(stop)
	mustNotFire(t, wi.Invalidated(stop), "a chain with no invalidating member fired")
}

// The common case: exactly one member can invalidate.
func TestChainForwardsASingleInvalidatingMember(t *testing.T) {
	inv := newInvalidating("livepodip")
	chain := CreateFilterChain([]filter.ObjectFilter{&plainFilter{"servicetopology"}, inv})

	stop := make(chan struct{})
	defer close(stop)
	ch := chain.(filter.WatchInvalidator).Invalidated(stop)

	mustNotFire(t, ch, "chain fired before its member did")
	close(inv.ch)
	mustFire(t, ch, "chain did not forward its only invalidating member's signal")
}

// Order must not matter: the chain order for a given key comes out of an
// unsorted set.
func TestChainForwardsRegardlessOfPosition(t *testing.T) {
	for _, first := range []bool{true, false} {
		inv := newInvalidating("livepodip")
		members := []filter.ObjectFilter{inv, &plainFilter{"servicetopology"}}
		if !first {
			members[0], members[1] = members[1], members[0]
		}
		chain := CreateFilterChain(members)

		stop := make(chan struct{})
		ch := chain.(filter.WatchInvalidator).Invalidated(stop)
		close(inv.ch)
		mustFire(t, ch, "chain did not forward the signal in this member order")
		close(stop)
	}
}

// With two or more, ANY one firing must invalidate — waiting for all of them
// would mean a single quiet member suppresses every other one's signal.
func TestChainFiresWhenAnyOfSeveralMembersFires(t *testing.T) {
	for _, which := range []int{0, 1} {
		a, b := newInvalidating("a"), newInvalidating("b")
		chain := CreateFilterChain([]filter.ObjectFilter{a, b})

		stop := make(chan struct{})
		ch := chain.(filter.WatchInvalidator).Invalidated(stop)

		if which == 0 {
			close(a.ch)
		} else {
			close(b.ch)
		}
		mustFire(t, ch, "chain did not fire when one of two members did")
		close(stop)
	}
}

// Both firing must not panic on a double close of the fan-in channel.
func TestChainSurvivesEveryMemberFiring(t *testing.T) {
	a, b := newInvalidating("a"), newInvalidating("b")
	chain := CreateFilterChain([]filter.ObjectFilter{a, b})

	stop := make(chan struct{})
	defer close(stop)
	ch := chain.(filter.WatchInvalidator).Invalidated(stop)

	close(a.ch)
	close(b.ch)
	mustFire(t, ch, "chain did not fire")
	time.Sleep(200 * time.Millisecond) // a double close would have panicked by now
}

// The fan-in goroutines must exit with the caller even if no member ever fires,
// or every watch on the node leaks two goroutines for the process's lifetime.
func TestChainFanInGoroutinesExitOnStop(t *testing.T) {
	before := runtime.NumGoroutine()

	for i := 0; i < 50; i++ {
		a, b := newInvalidating("a"), newInvalidating("b")
		chain := CreateFilterChain([]filter.ObjectFilter{a, b})
		stop := make(chan struct{})
		chain.(filter.WatchInvalidator).Invalidated(stop)
		close(stop)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+5 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("goroutines leaked: %d before, %d after 50 stopped chains", before, runtime.NumGoroutine())
}
