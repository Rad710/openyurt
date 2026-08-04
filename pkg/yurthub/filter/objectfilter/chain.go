/*
Copyright 2024 The OpenYurt Authors.

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
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	yurtutil "github.com/openyurtio/openyurt/pkg/util"
	"github.com/openyurtio/openyurt/pkg/yurthub/filter"
)

type filterChain []filter.ObjectFilter

func CreateFilterChain(objFilters []filter.ObjectFilter) filter.ObjectFilter {
	chain := make(filterChain, 0)
	chain = append(chain, objFilters...)
	return chain
}

func (chain filterChain) Name() string {
	var names []string
	for i := range chain {
		names = append(names, chain[i].Name())
	}
	return strings.Join(names, ",")
}

func (chain filterChain) SupportedResourceAndVerbs() map[string]sets.Set[string] {
	// do nothing
	return map[string]sets.Set[string]{}
}

func (chain filterChain) Filter(obj runtime.Object, stopCh <-chan struct{}) runtime.Object {
	for i := range chain {
		obj = chain[i].Filter(obj, stopCh)
		if yurtutil.IsNil(obj) {
			break
		}
	}

	return obj
}

// Invalidated implements filter.WatchInvalidator on behalf of any member that
// implements it, so a chain does not swallow a member's invalidation signal.
// The watch layer only ever sees the composed chain, so without this the signal
// would never reach it.
//
// Returns a channel that closes when ANY implementing member's does. The common
// case is exactly one implementer, which is handled without spawning anything.
func (chain filterChain) Invalidated(stop <-chan struct{}) <-chan struct{} {
	var sources []<-chan struct{}
	for i := range chain {
		if wi, ok := chain[i].(filter.WatchInvalidator); ok {
			if ch := wi.Invalidated(stop); ch != nil {
				sources = append(sources, ch)
			}
		}
	}

	switch len(sources) {
	case 0:
		// Never closed: no member can invalidate, so a watch waiting on this
		// simply never fires. A nil channel would also block forever but reads
		// as an accident.
		return make(chan struct{})
	case 1:
		return sources[0]
	}

	// Fan-in. One goroutine, bounded by stop so it cannot outlive the caller
	// even if no source ever fires.
	out := make(chan struct{})
	var once sync.Once
	closeOut := func() { once.Do(func() { close(out) }) }
	for i := range sources {
		src := sources[i]
		go func() {
			select {
			case <-src:
				closeOut()
			case <-stop:
			}
		}()
	}
	return out
}
