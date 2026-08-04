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

package cri

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// fakeSandbox is one entry of a fakeRuntime's fixture.
type fakeSandbox struct {
	id        string
	namespace string
	name      string
	state     runtimeapi.PodSandboxState
	createdAt int64
	ip        string // empty means "no network status", e.g. a hostNetwork pod
}

// fakeRuntime is a minimal in-process CRI RuntimeService that answers exactly
// the two RPCs this package calls, from a fixture set by each test.
type fakeRuntime struct {
	runtimeapi.UnimplementedRuntimeServiceServer

	mu        sync.Mutex
	sandboxes []fakeSandbox
	listCalls atomic.Int32

	// statusErr, if set, makes PodSandboxStatus fail for the sandbox with
	// this ID exactly, simulating one sandbox's status being unavailable
	// without affecting any other.
	statusErrForID string

	// listErr, if set, makes ListPodSandbox fail, simulating an unreachable
	// container runtime.
	listErr error
}

func (f *fakeRuntime) ListPodSandbox(_ context.Context, _ *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	f.listCalls.Add(1)
	f.mu.Lock()
	listErr := f.listErr
	f.mu.Unlock()
	if listErr != nil {
		return nil, listErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	items := make([]*runtimeapi.PodSandbox, 0, len(f.sandboxes))
	for _, sb := range f.sandboxes {
		items = append(items, &runtimeapi.PodSandbox{
			Id: sb.id,
			Metadata: &runtimeapi.PodSandboxMetadata{
				Namespace: sb.namespace,
				Name:      sb.name,
			},
			State:     sb.state,
			CreatedAt: sb.createdAt,
		})
	}
	return &runtimeapi.ListPodSandboxResponse{Items: items}, nil
}

func (f *fakeRuntime) PodSandboxStatus(_ context.Context, req *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusErrForID != "" && req.PodSandboxId == f.statusErrForID {
		return nil, errors.New("simulated status failure")
	}
	for _, sb := range f.sandboxes {
		if sb.id != req.PodSandboxId {
			continue
		}
		status := &runtimeapi.PodSandboxStatus{
			Id:        sb.id,
			Metadata:  &runtimeapi.PodSandboxMetadata{Namespace: sb.namespace, Name: sb.name},
			State:     sb.state,
			CreatedAt: sb.createdAt,
		}
		if sb.ip != "" {
			status.Network = &runtimeapi.PodSandboxNetworkStatus{Ip: sb.ip}
		}
		return &runtimeapi.PodSandboxStatusResponse{Status: status}, nil
	}
	return nil, fmt.Errorf("no such sandbox: %s", req.PodSandboxId)
}

// newTestSource starts fakeRuntime on a real unix socket (matching how
// NewSource is actually used — a filesystem path, not an in-memory dialer)
// and returns a Source dialed against it, cleaned up on test end.
func newTestSource(t *testing.T, fr *fakeRuntime, ttl time.Duration) Source {
	t.Helper()

	sockPath := filepath.Join(t.TempDir(), "cri.sock")
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(srv, fr)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	src, err := NewSource("unix://"+sockPath, ttl)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	return src
}

func TestPodIP_Found(t *testing.T) {
	fr := &fakeRuntime{sandboxes: []fakeSandbox{
		{id: "s1", namespace: "default", name: "web-1", state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 100, ip: "10.244.1.11"},
	}}
	src := newTestSource(t, fr, time.Minute)

	ip, found, err := src.PodIP(context.Background(), "default", "web-1")
	if err != nil {
		t.Fatalf("PodIP: %v", err)
	}
	if !found || ip != "10.244.1.11" {
		t.Fatalf("got ip=%q found=%v, want ip=10.244.1.11 found=true", ip, found)
	}
}

func TestPodIP_NotFound(t *testing.T) {
	fr := &fakeRuntime{sandboxes: []fakeSandbox{
		{id: "s1", namespace: "default", name: "web-1", state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 100, ip: "10.244.1.11"},
	}}
	src := newTestSource(t, fr, time.Minute)

	_, found, err := src.PodIP(context.Background(), "default", "does-not-exist")
	if err != nil {
		t.Fatalf("PodIP: %v", err)
	}
	if found {
		t.Fatalf("expected found=false for a pod with no sandbox")
	}
}

func TestPodIP_NotReadySandboxIgnored(t *testing.T) {
	fr := &fakeRuntime{sandboxes: []fakeSandbox{
		{id: "s1", namespace: "default", name: "web-1", state: runtimeapi.PodSandboxState_SANDBOX_NOTREADY, createdAt: 100, ip: "10.244.1.11"},
	}}
	src := newTestSource(t, fr, time.Minute)

	_, found, err := src.PodIP(context.Background(), "default", "web-1")
	if err != nil {
		t.Fatalf("PodIP: %v", err)
	}
	if found {
		t.Fatalf("a not-ready sandbox must not be reported as an answer")
	}
}

// TestPodIP_HostNetworkPodHasNoAnswer models a pod for which CRI reports no
// network status at all (the documented behaviour for hostNetwork pods): the
// source must say "no answer", never fabricate an address.
func TestPodIP_HostNetworkPodHasNoAnswer(t *testing.T) {
	fr := &fakeRuntime{sandboxes: []fakeSandbox{
		{id: "s1", namespace: "default", name: "host-pod", state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 100, ip: ""},
	}}
	src := newTestSource(t, fr, time.Minute)

	_, found, err := src.PodIP(context.Background(), "default", "host-pod")
	if err != nil {
		t.Fatalf("PodIP: %v", err)
	}
	if found {
		t.Fatalf("a sandbox with no reported network status must not be answered")
	}
}

// TestPodIP_NewestSandboxWins is the reboot case: a dead pre-reboot sandbox
// and its live replacement can momentarily coexist under the same pod
// identity. ListPodSandbox ordering is not a documented guarantee, so this
// must hold regardless of which one the fake lists first.
func TestPodIP_NewestSandboxWins(t *testing.T) {
	older := fakeSandbox{id: "old", namespace: "default", name: "web-1", state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 100, ip: "10.244.1.8"}
	newer := fakeSandbox{id: "new", namespace: "default", name: "web-1", state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 200, ip: "10.244.1.11"}

	for _, order := range [][]fakeSandbox{{older, newer}, {newer, older}} {
		fr := &fakeRuntime{sandboxes: order}
		src := newTestSource(t, fr, time.Minute)

		ip, found, err := src.PodIP(context.Background(), "default", "web-1")
		if err != nil {
			t.Fatalf("PodIP: %v", err)
		}
		if !found || ip != "10.244.1.11" {
			t.Fatalf("order=%v: got ip=%q found=%v, want the newer sandbox's ip=10.244.1.11", order, ip, found)
		}
	}
}

// TestPodIP_OneSandboxStatusFailureDoesNotBlankOthers proves a single failed
// PodSandboxStatus call cannot take down the answer for every other pod.
func TestPodIP_OneSandboxStatusFailureDoesNotBlankOthers(t *testing.T) {
	fr := &fakeRuntime{
		sandboxes: []fakeSandbox{
			{id: "broken", namespace: "default", name: "flaky", state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 100, ip: "10.244.1.5"},
			{id: "ok", namespace: "default", name: "web-1", state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 100, ip: "10.244.1.11"},
		},
		statusErrForID: "broken",
	}
	src := newTestSource(t, fr, time.Minute)

	if _, found, err := src.PodIP(context.Background(), "default", "flaky"); err != nil || found {
		t.Fatalf("flaky: found=%v err=%v, want found=false, err=nil", found, err)
	}
	ip, found, err := src.PodIP(context.Background(), "default", "web-1")
	if err != nil || !found || ip != "10.244.1.11" {
		t.Fatalf("web-1: got ip=%q found=%v err=%v, want 10.244.1.11/true/nil", ip, found, err)
	}
}

// TestPodIP_CacheTTLCollapsesLookups is the reason for the cache: looking up
// several pods within the TTL window must cost exactly one ListPodSandbox
// round trip, not one per pod.
func TestPodIP_CacheTTLCollapsesLookups(t *testing.T) {
	fr := &fakeRuntime{sandboxes: []fakeSandbox{
		{id: "s1", namespace: "default", name: "web-1", state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 100, ip: "10.244.1.11"},
		{id: "s2", namespace: "default", name: "web-2", state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 100, ip: "10.244.1.12"},
	}}
	src := newTestSource(t, fr, time.Minute)

	for _, name := range []string{"web-1", "web-2", "web-1", "web-2"} {
		if _, _, err := src.PodIP(context.Background(), "default", name); err != nil {
			t.Fatalf("PodIP(%s): %v", name, err)
		}
	}
	if got := fr.listCalls.Load(); got != 1 {
		t.Fatalf("ListPodSandbox called %d times, want exactly 1 within the TTL window", got)
	}
}

// TestPodIP_RefreshesAfterTTLExpires confirms the cache is not permanent: an
// address change (e.g. after a reboot) must be observed once the TTL passes.
func TestPodIP_RefreshesAfterTTLExpires(t *testing.T) {
	fr := &fakeRuntime{sandboxes: []fakeSandbox{
		{id: "s1", namespace: "default", name: "web-1", state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 100, ip: "10.244.1.8"},
	}}
	src := newTestSource(t, fr, 20*time.Millisecond)

	ip, _, err := src.PodIP(context.Background(), "default", "web-1")
	if err != nil || ip != "10.244.1.8" {
		t.Fatalf("first read: got ip=%q err=%v, want 10.244.1.8/nil", ip, err)
	}

	fr.mu.Lock()
	fr.sandboxes[0].ip = "10.244.1.11"
	fr.mu.Unlock()

	time.Sleep(40 * time.Millisecond)

	ip, _, err = src.PodIP(context.Background(), "default", "web-1")
	if err != nil || ip != "10.244.1.11" {
		t.Fatalf("after TTL: got ip=%q err=%v, want 10.244.1.11/nil (the cache should have refreshed)", ip, err)
	}
}

// TestPodIP_ConcurrentLookupsCollapseIntoOneRefresh is the concurrency
// analogue of the TTL test: many goroutines racing past a stale cache must
// still trigger only one refresh, via singleflight.
func TestPodIP_ConcurrentLookupsCollapseIntoOneRefresh(t *testing.T) {
	fr := &fakeRuntime{sandboxes: []fakeSandbox{
		{id: "s1", namespace: "default", name: "web-1", state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 100, ip: "10.244.1.11"},
	}}
	src := newTestSource(t, fr, time.Hour) // long TTL: only the initial stale state should trigger a refresh

	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := src.PodIP(context.Background(), "default", "web-1"); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent PodIP failed: %v", err)
	}
	if got := fr.listCalls.Load(); got != 1 {
		t.Fatalf("ListPodSandbox called %d times across 50 concurrent callers, want exactly 1", got)
	}
}

// --- change detection, which drives livepodip's watch invalidation ---

// The serve path (ensureFresh, on the cache TTL) and the change detector
// (Refresh, on a slower poll) share one refresh. If a serve-path refresh
// observes the address change first, Refresh must STILL report it — otherwise
// livepodip never invalidates open watches and consumers keep serving dead
// addresses for the whole outage.
func TestServePathDoesNotSwallowTheChangeSignal(t *testing.T) {
	fr := &fakeRuntime{sandboxes: []fakeSandbox{
		{id: "a", namespace: "default", name: "web-1",
			state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 1, ip: "10.244.1.11"},
	}}
	s := newTestSource(t, fr, 10*time.Millisecond)
	ctx := context.Background()

	if _, _, err := s.PodIP(ctx, "default", "web-1"); err != nil {
		t.Fatalf("prime: %v", err)
	}
	if _, err := s.Refresh(ctx); err != nil { // drain the initial-population latch
		t.Fatalf("drain: %v", err)
	}

	// The pod is replaced at a new address, as after a disconnected reboot.
	fr.mu.Lock()
	fr.sandboxes = []fakeSandbox{
		{id: "b", namespace: "default", name: "web-1",
			state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 2, ip: "10.244.1.31"},
	}
	fr.mu.Unlock()

	// A serve-path lookup gets there first.
	time.Sleep(20 * time.Millisecond) // let the TTL lapse
	ip, _, err := s.PodIP(ctx, "default", "web-1")
	if err != nil {
		t.Fatalf("serve lookup: %v", err)
	}
	if ip != "10.244.1.31" {
		t.Fatalf("serve path saw %q, want 10.244.1.31", ip)
	}

	changed, err := s.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if !changed {
		t.Fatal("Refresh reported changed=false after a serve-path refresh consumed the change")
	}
}

// Consuming is one-shot: a second Refresh with nothing new must report false,
// or every poll would invalidate and clients would re-LIST in a hot loop.
func TestRefreshConsumesTheChangeOnce(t *testing.T) {
	fr := &fakeRuntime{sandboxes: []fakeSandbox{
		{id: "a", namespace: "default", name: "web-1",
			state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 1, ip: "10.244.1.11"},
	}}
	s := newTestSource(t, fr, time.Nanosecond)
	ctx := context.Background()

	if _, err := s.Refresh(ctx); err != nil {
		t.Fatalf("first: %v", err)
	}
	changed, err := s.Refresh(ctx)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if changed {
		t.Fatal("Refresh reported changed=true twice for one change — this is a re-LIST hot loop")
	}
}

// Only addresses matter. A sandbox replaced by a newer one at the SAME address
// is not something any consumer needs to re-LIST for.
func TestRefreshIgnoresSandboxChurnAtTheSameAddress(t *testing.T) {
	fr := &fakeRuntime{sandboxes: []fakeSandbox{
		{id: "a", namespace: "default", name: "web-1",
			state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 1, ip: "10.244.1.11"},
	}}
	s := newTestSource(t, fr, time.Nanosecond)
	ctx := context.Background()

	if _, err := s.Refresh(ctx); err != nil {
		t.Fatalf("prime: %v", err)
	}

	fr.mu.Lock()
	fr.sandboxes = []fakeSandbox{
		{id: "b", namespace: "default", name: "web-1",
			state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 99, ip: "10.244.1.11"},
	}
	fr.mu.Unlock()

	changed, err := s.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if changed {
		t.Fatal("a new sandbox at the same address was reported as a change")
	}
}

// A failed refresh must not clear a change nobody has consumed yet.
func TestRefreshErrorDoesNotDropAPendingChange(t *testing.T) {
	fr := &fakeRuntime{sandboxes: []fakeSandbox{
		{id: "a", namespace: "default", name: "web-1",
			state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 1, ip: "10.244.1.11"},
	}}
	s := newTestSource(t, fr, time.Nanosecond)
	ctx := context.Background()

	// Populate without consuming: the serve path latches the change.
	if _, _, err := s.PodIP(ctx, "default", "web-1"); err != nil {
		t.Fatalf("prime: %v", err)
	}

	// An unchanged refresh in between must leave the latch alone.
	if _, _, err := s.PodIP(ctx, "default", "web-1"); err != nil {
		t.Fatalf("second lookup: %v", err)
	}

	changed, err := s.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if !changed {
		t.Fatal("an intervening unchanged refresh cleared a pending change")
	}
}

// An unreachable container runtime must be visible at default verbosity, and
// must be reported once per transition rather than once per retry — the poller
// retries for the length of an outage, on a node whose disk we care about.
func TestRuntimeFailureIsReportedOncePerTransition(t *testing.T) {
	fr := &fakeRuntime{sandboxes: []fakeSandbox{
		{id: "a", namespace: "default", name: "web-1",
			state: runtimeapi.PodSandboxState_SANDBOX_READY, createdAt: 1, ip: "10.244.1.11"},
	}}
	s := newTestSource(t, fr, time.Nanosecond).(*source)
	ctx := context.Background()

	if _, err := s.Refresh(ctx); err != nil {
		t.Fatalf("healthy refresh: %v", err)
	}
	if s.isRefreshFailing() {
		t.Fatal("a healthy refresh was recorded as failing")
	}

	// Break the runtime.
	fr.mu.Lock()
	fr.listErr = errors.New("connection refused")
	fr.mu.Unlock()

	for i := 0; i < 3; i++ {
		if _, err := s.Refresh(ctx); err == nil {
			t.Fatalf("refresh %d succeeded against a broken runtime", i)
		}
		if !s.isRefreshFailing() {
			t.Fatalf("refresh %d did not record the runtime as failing", i)
		}
	}

	// Recover it.
	fr.mu.Lock()
	fr.listErr = nil
	fr.mu.Unlock()

	if _, err := s.Refresh(ctx); err != nil {
		t.Fatalf("refresh after recovery: %v", err)
	}
	if s.isRefreshFailing() {
		t.Fatal("recovery was not recorded")
	}
}
