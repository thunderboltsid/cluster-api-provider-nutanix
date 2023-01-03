package context

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/cluster-api/errors"

	infrav1 "github.com/nutanix-cloud-native/cluster-api-provider-nutanix/api/v1beta1"
)

func TestClusterContext(t *testing.T) {
	cCtx := ClusterContext{
		NutanixCluster: &infrav1.NutanixCluster{
			Status: infrav1.NutanixClusterStatus{},
		},
	}
	cCtx.SetFailureStatus("error", fmt.Errorf("error"))
	assert.Equal(t, errors.ClusterStatusError("error"), *cCtx.NutanixCluster.Status.FailureReason)
	assert.Equal(t, "error", *cCtx.NutanixCluster.Status.FailureMessage)
}
