/*
Copyright 2021 The OpenYurt Authors.

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

package filter

import (
	"io"
	"net/http"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type NodesInPoolGetter func(poolName string) ([]string, error)

type Initializer interface {
	Initialize(filter ObjectFilter) error
}

// Approver check the response of specified request need to go through filter or not.
// and get all filters' names for the specified request.
type Approver interface {
	Approve(req *http.Request) (bool, []string)
}

// ResponseFilter is used for filtering response for get/list/watch requests.
// it only prepares the common framework for ObjectFilter and don't cover
// the filter logic.
type ResponseFilter interface {
	Name() string
	// Filter is used to filter data returned from the cloud.
	Filter(req *http.Request, rc io.ReadCloser, stopCh <-chan struct{}) (int, io.ReadCloser, error)
}

// ObjectFilter is used for filtering runtime object.
// runtime object is only a standalone object(like Service).
// Every Filter need to implement ObjectFilter interface.
type ObjectFilter interface {
	Name() string
	// Filter is used for filtering runtime object
	// all filter logic should be located in it.
	Filter(obj runtime.Object, stopCh <-chan struct{}) runtime.Object
}

type FilterFinder interface {
	FindResponseFilter(req *http.Request) (ResponseFilter, bool)
	FindObjectFilter(req *http.Request) (ObjectFilter, bool)
	ResourceSyncer
}

// WatchInvalidator is an optional interface an ObjectFilter may implement when
// its output for a given input object can change over time *independently of
// that object changing* — because it is derived from something outside the
// apiserver, such as the node's container runtime.
//
// It exists because a serve-time filter is only applied when an object is
// served. While the cloud is unreachable the multiplexer's cacher is fed from an
// unchanging disk cache and emits no watch events, so a client that has finished
// its initial LIST would otherwise keep a stale view indefinitely. Verified: a
// clean watch close makes client-go re-establish a watch, NOT re-LIST — only an
// error on the watch triggers a re-LIST (see
// pkg/yurthub/multiplexer/reflector_contract_test.go).
//
// A filter implementing this is telling the watch layer "terminate open watches
// so clients re-LIST and see my corrected output".
type WatchInvalidator interface {
	// Invalidated returns a channel that is closed when this filter's output for
	// already-served objects may have changed. The returned channel is valid for
	// one signal only; a caller wanting to observe further changes must call
	// Invalidated again.
	//
	// stop bounds any bookkeeping the implementation does on the caller's behalf
	// and must be closed by the caller when it stops caring. Implementations must
	// never return nil.
	Invalidated(stop <-chan struct{}) <-chan struct{}
}

type NodeGetter func(name string) (*v1.Node, error)

// ResourceSyncer is used for verifying the resources which filter depends on has been synced or not.
// For example: servicetopology filter depends on service and nodebucket metadata, filter can be worked
// before all these metadata has been synced completely.
type ResourceSyncer interface {
	HasSynced() bool
}
