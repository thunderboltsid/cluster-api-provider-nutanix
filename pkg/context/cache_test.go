package context

import (
	"net/http"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/keploy/go-sdk/integrations/khttpclient"
	"github.com/keploy/go-sdk/keploy"
	"github.com/keploy/go-sdk/mock"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRemoteClientCache(t *testing.T) {
	clusterKey := apitypes.NamespacedName{
		Namespace: "capx-test-ns",
		Name:      "mycluster",
	}

	// get absolute path for a relative directory path
	usr, err := user.Current()
	require.NoError(t, err)

	kubeconfigPath := filepath.Join(usr.HomeDir, ".kube/config")
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	require.NoError(t, err)

	mockConf := mock.Config{
		Mode: keploy.MODE_TEST,
		Name: t.Name(),
	}

	ctx := mock.NewContext(mockConf)
	cfg.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		interceptor := khttpclient.NewInterceptor(rt)
		interceptor.SetContext(ctx)
		return interceptor
	}

	kclnt, err := client.New(cfg, client.Options{})
	require.NoError(t, err)

	// create a new remote client as one doesn't exist
	c, err := GetRemoteClient(ctx, kclnt, clusterKey)
	assert.NoError(t, err)
	assert.NotNil(t, c)

	// get the remote client from the cache
	c, err = GetRemoteClient(ctx, kclnt, clusterKey)
	assert.NoError(t, err)
	assert.NotNil(t, c)

	// remove the remote client from the cache
	RemoveRemoteClient(clusterKey)
}
