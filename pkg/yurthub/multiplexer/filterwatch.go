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

package multiplexer

import (
	"net/http"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"

	yurtutil "github.com/openyurtio/openyurt/pkg/util"
	"github.com/openyurtio/openyurt/pkg/yurthub/filter"
)

type filterWatch struct {
	source watch.Interface
	filter filter.ObjectFilter
	result chan watch.Event
	done   chan struct{}

	// Stop has two callers per watch — receive()'s deferred Stop, and the
	// apiserver serving goroutine's deferred watcher.Stop in
	// handlers.ListResource — so it must be safe to call concurrently. The
	// previous check-then-close was not: both callers could observe done open
	// and both reach close, panicking with "close of closed channel". On this
	// fork that panic can kill yurthub on a site server in the middle of an
	// outage, which is worse than anything it would be protecting against.
	//
	// The window was always there; invalidation samples it far more often,
	// because every broadcast ends every open watch through receive().
	stopOnce sync.Once
}

func (f *filterWatch) Stop() {
	f.stopOnce.Do(func() {
		close(f.done)
		f.source.Stop()
	})
}

func newFilterWatch(source watch.Interface, filter filter.ObjectFilter) watch.Interface {
	if filter == nil {
		return source
	}

	fw := &filterWatch{
		source: source,
		filter: filter,
		result: make(chan watch.Event),
		done:   make(chan struct{}),
	}

	go fw.receive()

	return fw
}

func (f *filterWatch) ResultChan() <-chan watch.Event {
	return f.result
}

func (f *filterWatch) receive() {
	defer utilruntime.HandleCrash()
	defer close(f.result)
	defer f.Stop()

	// If the filter's output can change independently of the objects it is given
	// — livepodip's does, because it derives addresses from the container
	// runtime — it can tell us so, and we end the watch with an error. That is
	// the only thing that makes client-go re-LIST (a clean close just makes it
	// re-establish the watch); a re-LIST re-runs the filter and the client
	// finally sees corrected objects. Pinned by
	// reflector_contract_test.go.
	var invalidated <-chan struct{}
	if wi, ok := f.filter.(filter.WatchInvalidator); ok {
		invalidated = wi.Invalidated(f.done)
	}

	source := f.source.ResultChan()
	for {
		select {
		case <-f.done:
			return

		case <-invalidated:
			f.sendExpired()
			return

		case result, ok := <-source:
			if !ok {
				return
			}

			watchType := result.Type
			newObj := result.Object
			if co, ok := newObj.(runtime.CacheableObject); ok {
				newObj = co.GetObject()
			}

			if result.Type != watch.Bookmark && result.Type != watch.Error {
				if newObj = f.filter.Filter(newObj, f.done); yurtutil.IsNil(newObj) {
					watchType = watch.Deleted
					newObj = result.Object
				}
			}

			select {
			case <-f.done:
				return
			case f.result <- watch.Event{
				Type:   watchType,
				Object: newObj,
			}:
			}
		}
	}
}

// sendExpired ends the watch the way an apiserver signals "your view is
// unusable, start over": a 410 Gone / Expired status as a watch error. client-go
// turns this into an error from its watch handler, which makes the reflector
// return from ListAndWatch and perform a fresh LIST.
func (f *filterWatch) sendExpired() {
	klog.V(2).Infof("filter %s invalidated its output, ending watch with 410 so the client re-lists", f.filter.Name())
	select {
	case <-f.done:
	case f.result <- watch.Event{
		Type: watch.Error,
		Object: &metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    http.StatusGone,
			Reason:  metav1.StatusReasonExpired,
			Message: "yurthub: cached objects for this resource were corrected while disconnected from the cloud, please re-list",
		},
	}:
	}
}
