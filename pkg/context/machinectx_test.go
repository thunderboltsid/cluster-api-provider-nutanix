package context

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	capiv1 "sigs.k8s.io/cluster-api/api/v1beta1"
	"sigs.k8s.io/cluster-api/errors"

	infrav1 "github.com/nutanix-cloud-native/cluster-api-provider-nutanix/api/v1beta1"
)

func TestMachineContext(t *testing.T) {
	cCtx := MachineContext{
		NutanixMachine: &infrav1.NutanixMachine{
			Status: infrav1.NutanixMachineStatus{},
		},
	}
	cCtx.SetFailureStatus("error", fmt.Errorf("error"))
	assert.Equal(t, errors.MachineStatusError("error"), *cCtx.NutanixMachine.Status.FailureReason)
	assert.Equal(t, "error", *cCtx.NutanixMachine.Status.FailureMessage)
}

func TestIsControlPlaneMachine(t *testing.T) {
	machine := &infrav1.NutanixMachine{
		ObjectMeta: v1.ObjectMeta{
			Labels: make(map[string]string),
		},
	}
	assert.False(t, IsControlPlaneMachine(machine))

	machine.ObjectMeta.Labels[capiv1.MachineControlPlaneLabelName] = "true"
	assert.True(t, IsControlPlaneMachine(machine))
}
