package context

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestGetRemoteClient(t *testing.T) {
	clusterKey := apitypes.NamespacedName{
		Namespace: "test-namespace",
		Name:      "test-cluster",
	}
	cfg := &rest.Config{}
	kclnt, err := client.New(cfg, client.Options{})
	require.NoError(t, err)
	c, err := GetRemoteClient(context.Background(), kclnt, clusterKey)
	assert.NoError(t, err)
	assert.NotNil(t, c)
}

func TestRemoveRemoteClient(t *testing.T) {
	t.Log("TODO: Implement")
}
