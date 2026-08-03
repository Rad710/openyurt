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

package initializer

import (
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	"github.com/openyurtio/openyurt/pkg/yurthub/filter"
	"github.com/openyurtio/openyurt/pkg/yurthub/healthchecker"
	"github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/cri"
)

// WantsSharedInformerFactory is an interface for setting SharedInformerFactory
type WantsSharedInformerFactory interface {
	SetSharedInformerFactory(factory informers.SharedInformerFactory) error
}

// WantsNodeName is an interface for setting node name
type WantsNodeName interface {
	SetNodeName(nodeName string) error
}

// WantsNodePoolName is an interface for setting nodePool name
type WantsNodePoolName interface {
	SetNodePoolName(nodePoolName string) error
}

// WantsMasterServiceAddr is an interface for setting mutated master service address
type WantsMasterServiceAddr interface {
	SetMasterServiceHost(host string) error
	SetMasterServicePort(port string) error
}

// WantsKubeClient is an interface for setting kube client
type WantsKubeClient interface {
	SetKubeClient(client kubernetes.Interface) error
}

// WantsHealthChecker is an interface for setting the cloud health checker, so
// a filter can ask whether the cloud is currently reachable and, per
// decisions/0002, change its behaviour only while disconnected. Unlike the
// other Wants* interfaces here, it is NOT invoked by
// genericFilterInitializer.Initialize — the checker does not exist yet at
// filter-construction time (see filter.FilterFinder.SetHealthChecker for why)
// — so a filter implementing this must tolerate SetHealthChecker being called
// once, later, after construction, and must tolerate IsHealthy() never having
// been askable at all if the checker is nil (working modes other than Edge
// never attach one).
type WantsHealthChecker interface {
	SetHealthChecker(checker healthchecker.Interface) error
}

// WantsPodIPSource is an interface for setting the container-runtime pod-IP
// source, so a filter can learn what address a pod on this node actually has
// right now rather than what the cache last recorded. Unlike
// WantsHealthChecker above, this one IS injected through the normal
// Initialize chain: cri.NewSource dials lazily, so it can be constructed in
// config.Complete() alongside every other filter dependency. May be nil.
type WantsPodIPSource interface {
	SetPodIPSource(source cri.Source) error
}

// genericFilterInitializer is responsible for initializing generic filter
type genericFilterInitializer struct {
	factory           informers.SharedInformerFactory
	nodeName          string
	nodePoolName      string
	masterServiceHost string
	masterServicePort string
	client            kubernetes.Interface
	podIPSource       cri.Source
}

// New creates an filterInitializer object. podIPSource may be nil — only the
// edge working mode with a reachable container runtime has one.
func New(factory informers.SharedInformerFactory, kubeClient kubernetes.Interface, nodeName, nodePoolName, masterServiceHost, masterServicePort string, podIPSource cri.Source) filter.Initializer {
	return &genericFilterInitializer{
		factory:           factory,
		nodeName:          nodeName,
		nodePoolName:      nodePoolName,
		masterServiceHost: masterServiceHost,
		masterServicePort: masterServicePort,
		client:            kubeClient,
		podIPSource:       podIPSource,
	}
}

// Initialize used for executing filter initialization
func (fi *genericFilterInitializer) Initialize(ins filter.ObjectFilter) error {
	if wants, ok := ins.(WantsNodeName); ok {
		if err := wants.SetNodeName(fi.nodeName); err != nil {
			return err
		}
	}

	if wants, ok := ins.(WantsNodePoolName); ok {
		if err := wants.SetNodePoolName(fi.nodePoolName); err != nil {
			return err
		}
	}

	if wants, ok := ins.(WantsMasterServiceAddr); ok {
		if err := wants.SetMasterServiceHost(fi.masterServiceHost); err != nil {
			return err
		}

		if err := wants.SetMasterServicePort(fi.masterServicePort); err != nil {
			return err
		}
	}

	if wants, ok := ins.(WantsSharedInformerFactory); ok {
		if err := wants.SetSharedInformerFactory(fi.factory); err != nil {
			return err
		}
	}

	if wants, ok := ins.(WantsKubeClient); ok {
		if err := wants.SetKubeClient(fi.client); err != nil {
			return err
		}
	}

	if wants, ok := ins.(WantsPodIPSource); ok {
		if err := wants.SetPodIPSource(fi.podIPSource); err != nil {
			return err
		}
	}

	return nil
}
