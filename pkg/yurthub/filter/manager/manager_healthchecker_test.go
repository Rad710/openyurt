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

package manager

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/openyurtio/openyurt/pkg/yurthub/filter"
	"github.com/openyurtio/openyurt/pkg/yurthub/healthchecker"
	fakehealthchecker "github.com/openyurtio/openyurt/pkg/yurthub/healthchecker/fake"
)

// fakeHealthCheckerWantingFilter is the minimal filter used to prove
// Manager.SetHealthChecker actually reaches a filter that asks for the
// checker via initializer.WantsHealthChecker. No real OpenYurt filter
// implements that interface yet — it is added by this change for the
// serve-time EndpointSlice rewrite that consumes it later.
type fakeHealthCheckerWantingFilter struct {
	setCalls    int
	lastChecker healthchecker.Interface
	setErr      error
}

func (f *fakeHealthCheckerWantingFilter) Name() string { return "fake-health-checker-wanting" }

func (f *fakeHealthCheckerWantingFilter) Filter(obj runtime.Object, _ <-chan struct{}) runtime.Object {
	return obj
}

// SetHealthChecker's parameter type must be exactly healthchecker.Interface —
// not merely a structurally similar interface{ IsHealthy() bool } — or this
// type does not satisfy initializer.WantsHealthChecker and Manager silently
// skips it, which would make this test pass for the wrong reason.
func (f *fakeHealthCheckerWantingFilter) SetHealthChecker(checker healthchecker.Interface) error {
	f.setCalls++
	f.lastChecker = checker
	return f.setErr
}

// fakeFilterNotWantingHealthChecker proves SetHealthChecker skips filters
// that never asked for it, rather than erroring on the missing method.
type fakeFilterNotWantingHealthChecker struct{}

func (f *fakeFilterNotWantingHealthChecker) Name() string { return "fake-indifferent" }

func (f *fakeFilterNotWantingHealthChecker) Filter(obj runtime.Object, _ <-chan struct{}) runtime.Object {
	return obj
}

func TestManagerSetHealthChecker(t *testing.T) {
	wanting := &fakeHealthCheckerWantingFilter{}
	indifferent := &fakeFilterNotWantingHealthChecker{}
	m := &Manager{
		nameToObjectFilter: map[string]filter.ObjectFilter{
			"wanting":     wanting,
			"indifferent": indifferent,
		},
	}

	checker := fakehealthchecker.NewFakeChecker(nil)
	if err := m.SetHealthChecker(checker); err != nil {
		t.Fatalf("SetHealthChecker: %v", err)
	}

	if wanting.setCalls != 1 {
		t.Errorf("expected the WantsHealthChecker filter to receive the checker exactly once, got %d calls", wanting.setCalls)
	}
	if wanting.lastChecker != checker {
		t.Errorf("filter received a different checker instance than the one passed in")
	}
	// indifferent implements no Wants* interface, so nothing to assert beyond
	// SetHealthChecker not having errored above (it must not, e.g., panic on a
	// failed type assertion).
}

func TestManagerSetHealthChecker_NilCheckerIsNoOp(t *testing.T) {
	wanting := &fakeHealthCheckerWantingFilter{}
	m := &Manager{
		nameToObjectFilter: map[string]filter.ObjectFilter{"wanting": wanting},
	}

	if err := m.SetHealthChecker(nil); err != nil {
		t.Fatalf("SetHealthChecker(nil): %v", err)
	}
	if wanting.setCalls != 0 {
		t.Errorf("a nil checker (working modes other than Edge) must not reach any filter, got %d calls", wanting.setCalls)
	}
}

func TestManagerSetHealthChecker_PropagatesFilterError(t *testing.T) {
	wantErr := errors.New("boom")
	wanting := &fakeHealthCheckerWantingFilter{setErr: wantErr}
	m := &Manager{
		nameToObjectFilter: map[string]filter.ObjectFilter{"wanting": wanting},
	}

	err := m.SetHealthChecker(fakehealthchecker.NewFakeChecker(nil))
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the filter's own error to propagate, got %v", err)
	}
}
