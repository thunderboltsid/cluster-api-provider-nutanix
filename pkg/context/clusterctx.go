package context

import (
	"context"
	"fmt"

	"github.com/nutanix-cloud-native/prism-go-client/utils"
	prismv3 "github.com/nutanix-cloud-native/prism-go-client/v3"
	"k8s.io/klog/v2"
	capiv1 "sigs.k8s.io/cluster-api/api/v1beta1"
	capierrors "sigs.k8s.io/cluster-api/errors"

	infrav1 "github.com/nutanix-cloud-native/cluster-api-provider-nutanix/api/v1beta1"
)

// ClusterContext is a context used with a NutanixCluster reconciler
type ClusterContext struct {
	Context       context.Context
	NutanixClient *prismv3.Client

	Cluster        *capiv1.Cluster
	NutanixCluster *infrav1.NutanixCluster

	// The prefix to prepend to logs
	LogPrefix string
}

// SetFailureStatus sets the failure status on the NutanixCluster
func (clusterCtx *ClusterContext) SetFailureStatus(failureReason capierrors.ClusterStatusError, failureMessage error) {
	klog.Infof("Setting cluster failure status. Reason: %s, Message: %v", failureReason, failureMessage)
	clusterCtx.NutanixCluster.Status.FailureMessage = utils.StringPtr(fmt.Sprintf("%v", failureMessage))
	clusterCtx.NutanixCluster.Status.FailureReason = &failureReason
}
