/*
Copyright 2024 The OpenYurt Authors.
Copyright 2017 The Kubernetes Authors.

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
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	"k8s.io/apiserver/pkg/endpoints/handlers"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	v1 "k8s.io/kubernetes/pkg/apis/core/v1"

	hubmeta "github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/meta"
	"github.com/openyurtio/openyurt/pkg/yurthub/multiplexer"
	"github.com/openyurtio/openyurt/pkg/yurthub/util"
)

const (
	minRequestTimeout = 300 * time.Second
)

type multiplexerProxy struct {
	requestsMultiplexerManager *multiplexer.MultiplexerManager
	restMapperManager          *hubmeta.RESTMapperManager
	stop                       <-chan struct{}
}

func init() {
	// When parsing the FieldSelector in list/watch requests, the corresponding resource's conversion functions need to be used.
	// Here, the primary action is to introduce the conversion functions registered in the core/v1 resources into the scheme.
	v1.AddToScheme(scheme.Scheme)
}

func NewMultiplexerProxy(multiplexerManager *multiplexer.MultiplexerManager, restMapperMgr *hubmeta.RESTMapperManager, stop <-chan struct{}) http.Handler {
	return &multiplexerProxy{
		stop:                       stop,
		requestsMultiplexerManager: multiplexerManager,
		restMapperManager:          restMapperMgr,
	}
}

func (sp *multiplexerProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqInfo, _ := request.RequestInfoFrom(r.Context())
	gvr := &schema.GroupVersionResource{
		Group:    reqInfo.APIGroup,
		Version:  reqInfo.APIVersion,
		Resource: reqInfo.Resource,
	}

	if !sp.requestsMultiplexerManager.Ready(gvr) {
		w.Header().Set("Retry-After", "1")
		util.Err(apierrors.NewTooManyRequestsError(fmt.Sprintf("cacher for gvr(%s) is initializing, please try again later.", gvr.String())), w, r)
		return
	}

	if err := rejectWatchList(r); err != nil {
		util.Err(err, w, r)
		return
	}

	restStore, err := sp.requestsMultiplexerManager.ResourceStore(gvr)
	if err != nil {
		util.Err(errors.Wrapf(err, "failed to get rest storage"), w, r)
	}

	reqScope, err := sp.getReqScope(gvr)
	if err != nil {
		util.Err(errors.Wrapf(err, "failed tp get req scope"), w, r)
	}

	lister := restStore.(rest.Lister)
	watcher := restStore.(rest.Watcher)
	forceWatch := reqInfo.Verb == "watch"
	handlers.ListResource(lister, watcher, reqScope, forceWatch, minRequestTimeout).ServeHTTP(w, r)
}

// rejectWatchList turns away a WatchList (sendInitialEvents=true) request for
// pool-scope metadata, so the client falls back to LIST+WATCH.
//
// The equivalent already existed for the local disk-replay path
// (pkg/yurthub/proxy/local/local.go), but pool-scope resources — EndpointSlices
// among them — never touch that path, so it did not apply where it matters most.
// Measured on hardware 2026-08-04: 32 WatchList requests reached the multiplexer
// and every one was answered 200 in under a millisecond having delivered nothing
// at all, while 464 were correctly rejected on the local path.
//
// Answering 200 with no initial events and no "k8s.io/initial-events-end"
// bookmark is worse than an error. A client that treats it as a successful sync
// concludes the resource is empty; client-go itself falls back, but only because
// it checks for the missing bookmark, and any consumer that acts on "informer
// synced" before that check sees an empty world. An explicit error removes the
// ambiguity: client-go treats anything that is not a 429 or a connection refusal
// as a signal to use LIST+WATCH, which this proxy serves correctly and which is
// the path serve-time filters are applied on.
func rejectWatchList(r *http.Request) error {
	opts := metainternalversion.ListOptions{}
	if err := metainternalversionscheme.ParameterCodec.DecodeParameters(
		r.URL.Query(), metav1.SchemeGroupVersion, &opts); err != nil {
		return apierrors.NewBadRequest(err.Error())
	}
	if opts.SendInitialEvents == nil || !*opts.SendInitialEvents {
		return nil
	}

	klog.V(2).Infof("rejecting watchlist request for pool scope metadata %s, so the client falls back to list+watch", util.ReqString(r))
	return apierrors.NewBadRequest("yurthub does not support sendInitialEvents(WatchList) for pool scope metadata, use list+watch instead")
}

func (sp *multiplexerProxy) getReqScope(gvr *schema.GroupVersionResource) (*handlers.RequestScope, error) {
	_, fqKindToRegister := sp.restMapperManager.KindFor(*gvr)
	if fqKindToRegister.Empty() {
		return nil, fmt.Errorf("gvk is not found for gvr: %v", *gvr)
	}

	return &handlers.RequestScope{
		Serializer:      scheme.Codecs,
		ParameterCodec:  scheme.ParameterCodec,
		Convertor:       scheme.Scheme,
		Defaulter:       scheme.Scheme,
		Typer:           scheme.Scheme,
		UnsafeConvertor: runtime.UnsafeObjectConvertor(scheme.Scheme),
		Authorizer:      authorizerfactory.NewAlwaysAllowAuthorizer(),

		EquivalentResourceMapper: runtime.NewEquivalentResourceRegistry(),

		// TODO: Check for the interface on storage
		TableConvertor: rest.NewDefaultTableConvertor(gvr.GroupResource()),

		// TODO: This seems wrong for cross-group subresources. It makes an assumption that a subresource and its parent are in the same group version. Revisit this.
		Resource: *gvr,
		Kind:     fqKindToRegister,

		HubGroupVersion: schema.GroupVersion{Group: fqKindToRegister.Group, Version: runtime.APIVersionInternal},

		MetaGroupVersion: metav1.SchemeGroupVersion,

		MaxRequestBodyBytes: int64(3 * 1024 * 1024),
		Namer: handlers.ContextBasedNaming{
			Namer: runtime.Namer(meta.NewAccessor()),
		},
	}, nil
}
