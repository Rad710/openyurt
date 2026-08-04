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
	"net/http"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const slicesPath = "/apis/discovery.k8s.io/v1/endpointslices"

func watchListReq(t *testing.T, url string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	return r
}

// The case measured on hardware: 32 of these reached the multiplexer and were
// answered 200 having delivered nothing, because the rejection only existed on
// the local disk-replay path, which pool-scope resources never touch.
func TestRejectsWatchList(t *testing.T) {
	r := watchListReq(t, slicesPath+"?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan")

	err := rejectWatchList(r)
	if err == nil {
		t.Fatal("a WatchList request was accepted; the client would treat an empty 200 as a synced, empty world")
	}
	if !apierrors.IsBadRequest(err) {
		t.Errorf("error is %v, want a BadRequest — client-go falls back on any error that is not 429 or a connection refusal", err)
	}
}

// The exact query Traefik sent, taken verbatim from the 2026-08-04 journal.
func TestRejectsWatchListAsObservedOnHardware(t *testing.T) {
	r := watchListReq(t, slicesPath+
		"?allowWatchBookmarks=true&resourceVersionMatch=NotOlderThan&sendInitialEvents=true"+
		"&timeout=6m29s&timeoutSeconds=389&watch=true")

	if err := rejectWatchList(r); err == nil {
		t.Fatal("the request that produced the 200-with-nothing on hardware is still accepted")
	}
}

// Everything else must be untouched: a plain LIST and an ordinary WATCH are the
// paths that actually work, and are where serve-time filters are applied.
func TestAllowsListAndOrdinaryWatch(t *testing.T) {
	for _, url := range []string{
		slicesPath,
		slicesPath + "?resourceVersion=0&limit=500",
		slicesPath + "?resourceVersion=205360",
		slicesPath + "?watch=true&allowWatchBookmarks=true&resourceVersion=205360&timeoutSeconds=430",
		slicesPath + "?watch=true&sendInitialEvents=false",
	} {
		if err := rejectWatchList(watchListReq(t, url)); err != nil {
			t.Errorf("rejected a request it should serve (%s): %v", url, err)
		}
	}
}

// A malformed query must not be mistaken for "no sendInitialEvents".
func TestRejectsUndecodableParameters(t *testing.T) {
	if err := rejectWatchList(watchListReq(t, slicesPath+"?timeoutSeconds=abc")); err == nil {
		t.Error("undecodable list options were accepted")
	}
}
