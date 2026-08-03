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

// Package cri gives yurthub a way to learn a pod's current IP address
// directly from the container runtime, with no dependency on the apiserver.
//
// It exists because there is no other live source for this while
// disconnected: after a reboot the CNI assigns every pod a new address, but
// EndpointSlices are written only by the control-plane endpoint controller,
// which is unreachable, and kubelet's own offline pod-status writes are never
// read by yurthub's local cache path (see docs/edge-autonomy/decisions/0002
// for the full reasoning, including why teeing kubelet's writes was tried
// first and abandoned). This package is the pod-IP source that decision
// depends on; the filter that uses it to rewrite EndpointSlices at serve time
// is a separate, later change.
package cri

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	criutil "k8s.io/cri-client/pkg/util"
	"k8s.io/klog/v2"
)

const (
	// DefaultRuntimeEndpoint matches containerd's default socket, which is
	// what this fork's install sequence uses (docs/edge-autonomy/install.md).
	DefaultRuntimeEndpoint = "unix:///run/containerd/containerd.sock"

	// DefaultCacheTTL bounds how stale a PodIP answer can be. It exists so
	// that looking up every endpoint in one EndpointSlice LIST costs one
	// ListPodSandbox round trip plus one PodSandboxStatus per sandbox, not one
	// full refresh per endpoint.
	DefaultCacheTTL = 2 * time.Second

	dialTimeout = 10 * time.Second

	// refreshTimeout bounds a single ListPodSandbox+PodSandboxStatus round
	// trip against the runtime. It is deliberately NOT derived from any one
	// caller's context: singleflight can collapse several callers with
	// different contexts into one in-flight refresh (see ensureFresh), and
	// one caller's cancellation must not abort a refresh the others are still
	// waiting on.
	refreshTimeout = 5 * time.Second

	// maxMsgSize matches k8s.io/cri-client's own default (16MB; the grpc
	// library default of 4MB has been observed too small for some runtimes'
	// responses).
	maxMsgSize = 1024 * 1024 * 16
)

// PodIdentity names a pod sandbox on this node the way EndpointSlices name it
// (endpoints[].targetRef), not by IP — matching on identity rather than
// address is what lets a rewrite built on this source never return an
// address that has since been reassigned to a different pod.
type PodIdentity struct {
	Namespace string
	Name      string
}

func (id PodIdentity) String() string {
	return id.Namespace + "/" + id.Name
}

// Source reports the current IP address of pods running on this node, read
// directly from the container runtime over CRI.
type Source interface {
	// PodIP returns the IP most recently observed for the named pod's sandbox
	// on this node. found is false if no ready sandbox with a network address
	// was seen for that pod — including hostNetwork pods, for which CRI does
	// not report network status at all. A caller MUST treat found==false as
	// "this source has no answer", never as "the pod is not running": it must
	// not be used to justify serving a stale cached address, only to justify
	// leaving one alone.
	PodIP(ctx context.Context, namespace, name string) (ip string, found bool, err error)

	// Close releases the underlying connection to the container runtime.
	Close() error
}

type podEntry struct {
	ip        string
	createdAt int64
}

type source struct {
	client runtimeapi.RuntimeServiceClient
	conn   *grpc.ClientConn
	ttl    time.Duration
	group  singleflight.Group

	mu        sync.RWMutex
	fetchedAt time.Time
	byPod     map[PodIdentity]podEntry
}

// NewSource dials the container runtime at endpoint (e.g.
// "unix:///run/containerd/containerd.sock") and returns a Source. The dial
// itself does not block on the runtime being reachable yet — grpc connects
// lazily on the first call — so this can safely be constructed during yurthub
// startup even if containerd is not yet ready.
func NewSource(endpoint string, ttl time.Duration) (Source, error) {
	if endpoint == "" {
		endpoint = DefaultRuntimeEndpoint
	}
	if ttl <= 0 {
		ttl = DefaultCacheTTL
	}

	addr, dialer, err := criutil.GetAddressAndDialer(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parsing CRI runtime endpoint %q: %w", endpoint, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	// This mirrors k8s.io/cri-client's own NewRemoteRuntimeService dial
	// options exactly (pkg/remote_runtime.go), rather than the newer
	// grpc.NewClient: addr here is a raw filesystem path with no scheme, and
	// NewClient's default resolver for such a target is "dns", which would
	// try to resolve it as a hostname. grpc.DialContext plus
	// WithContextDialer sidesteps that — the dialer receives addr verbatim.
	conn, err := grpc.DialContext(ctx, addr, //nolint:staticcheck // SA1019: matches k8s.io/cri-client's own verified dial pattern
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithAuthority("localhost"),
		grpc.WithContextDialer(dialer),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("dialing CRI runtime at %q: %w", endpoint, err)
	}

	return &source{
		client: runtimeapi.NewRuntimeServiceClient(conn),
		conn:   conn,
		ttl:    ttl,
		byPod:  map[PodIdentity]podEntry{},
	}, nil
}

func (s *source) Close() error {
	return s.conn.Close()
}

func (s *source) PodIP(ctx context.Context, namespace, name string) (string, bool, error) {
	if err := s.ensureFresh(ctx); err != nil {
		return "", false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.byPod[PodIdentity{Namespace: namespace, Name: name}]
	return entry.ip, ok, nil
}

func (s *source) ensureFresh(_ context.Context) error {
	s.mu.RLock()
	fresh := time.Since(s.fetchedAt) < s.ttl
	s.mu.RUnlock()
	if fresh {
		return nil
	}

	// singleflight collapses every caller that finds the snapshot stale into
	// one refresh, so a burst of lookups for every endpoint in one
	// EndpointSlice LIST costs a single ListPodSandbox round trip.
	_, err, _ := s.group.Do("refresh", func() (interface{}, error) {
		refreshCtx, cancel := context.WithTimeout(context.Background(), refreshTimeout)
		defer cancel()
		return nil, s.refresh(refreshCtx)
	})
	return err
}

func (s *source) refresh(ctx context.Context) error {
	resp, err := s.client.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{})
	if err != nil {
		return fmt.Errorf("listing pod sandboxes: %w", err)
	}

	next := make(map[PodIdentity]podEntry, len(resp.GetItems()))
	for _, sb := range resp.GetItems() {
		if sb.GetState() != runtimeapi.PodSandboxState_SANDBOX_READY {
			continue // a not-ready sandbox has no meaningful network status
		}
		id := PodIdentity{Namespace: sb.GetMetadata().GetNamespace(), Name: sb.GetMetadata().GetName()}
		if id.Namespace == "" || id.Name == "" {
			continue
		}

		statusResp, err := s.client.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{PodSandboxId: sb.GetId()})
		if err != nil {
			// One sandbox failing to report status must not blank out the
			// answer for every other pod.
			klog.V(4).Infof("cri: PodSandboxStatus(%s) for %s failed, skipping: %v", sb.GetId(), id, err)
			continue
		}
		network := statusResp.GetStatus().GetNetwork()
		if network == nil || network.GetIp() == "" {
			// hostNetwork pods and anything else the runtime reports no
			// network status for. Not an error: this source simply has no
			// answer for them.
			continue
		}

		// Two sandboxes can momentarily share a pod identity across a
		// reboot: the dead pre-reboot sandbox and the fresh one. crictl/CRI's
		// ListPodSandbox ordering is not a documented guarantee, so this must
		// not depend on it — pick explicitly instead: newer CreatedAt wins.
		// Both candidates are already READY at this point (checked above).
		if existing, ok := next[id]; !ok || sb.GetCreatedAt() > existing.createdAt {
			next[id] = podEntry{ip: network.GetIp(), createdAt: sb.GetCreatedAt()}
		}
	}

	s.mu.Lock()
	s.byPod = next
	s.fetchedAt = time.Now()
	s.mu.Unlock()
	return nil
}
