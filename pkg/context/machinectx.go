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

// MachineContext is a context used with a NutanixMachine reconciler
type MachineContext struct {
	Context       context.Context
	NutanixClient *prismv3.Client

	Cluster        *capiv1.Cluster
	Machine        *capiv1.Machine
	NutanixCluster *infrav1.NutanixCluster
	NutanixMachine *infrav1.NutanixMachine

	// The VM ip address
	IP string

	// The prefix to prepend to logs
	LogPrefix string
}

// SetFailureStatus sets the failure status on the NutanixMachine
func (machineCtx *MachineContext) SetFailureStatus(failureReason capierrors.MachineStatusError, failureMessage error) {
	klog.Infof("Setting machine failure status. Reason: %s, Message: %v", failureReason, failureMessage)
	machineCtx.NutanixMachine.Status.FailureMessage = utils.StringPtr(fmt.Sprintf("%v", failureMessage))
	machineCtx.NutanixMachine.Status.FailureReason = &failureReason
}

// IsControlPlaneMachine returns true if the provided resource is
// a member of the control plane. It does so by looking for the
// label "cluster.x-k8s.io/control-plane" and checking if it is present.
func IsControlPlaneMachine(nm *infrav1.NutanixMachine) bool {
	if nm == nil {
		return false
	}
	_, ok := nm.GetLabels()[capiv1.MachineControlPlaneLabelName]
	return ok
}
