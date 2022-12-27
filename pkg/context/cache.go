/*
Copyright 2022 Nutanix

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

package context

import (
	"context"
	"sync"

	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-api/controllers/remote"
	ctlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	remoteClientCache map[ctlclient.ObjectKey]ctlclient.Client
	cacheMutex        *sync.Mutex
)

func init() {
	cacheMutex = &sync.Mutex{}
	remoteClientCache = make(map[ctlclient.ObjectKey]ctlclient.Client)
	klog.Infof("Initialized remote controller runtime client cache")
}

// GetRemoteClient returns a kubernetes controller runtime client for the remote cluster if it already exists.
// If a client does not exist in the cache, it creates one and adds it to the cache.
func GetRemoteClient(ctx context.Context, client ctlclient.Client, clusterKey ctlclient.ObjectKey) (ctlclient.Client, error) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	remoteClient, ok := remoteClientCache[clusterKey]
	if ok {
		return remoteClient, nil
	}

	remoteClient, err := remote.NewClusterClient(ctx, "remote-cluster-cache", client, clusterKey)
	if err != nil {
		klog.Errorf("Failed to create client for remote cluster %v", clusterKey)
		return nil, err
	}
	remoteClientCache[clusterKey] = remoteClient

	return remoteClient, nil
}

// RemoveRemoteClient removes the remote client from the cache.
func RemoveRemoteClient(clusterKey ctlclient.ObjectKey) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	delete(remoteClientCache, clusterKey)
}
