package context

import (
	"k8s.io/client-go/rest"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
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
	kubeconfigPath := filepath.Join("testdata", "kube.config")
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	require.NoError(t, err)
	rt, err := rest.TransportFor(cfg)
	require.NoError(t, err)
	cfg.TLSClientConfig = rest.TLSClientConfig{}

	mockConf := mock.Config{
		Mode: keploy.MODE_TEST,
		Name: t.Name(),
	}

	ctx := mock.NewContext(mockConf)
	interceptor := khttpclient.NewInterceptor(rt)
	interceptor.SetContext(ctx)
	cfg.Transport = interceptor
	//cfg.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
	//	return interceptor
	//}

	mapper, err := apiutil.NewDynamicRESTMapper(cfg, apiutil.WithLazyDiscovery)
	require.NoError(t, err)
	copts := client.Options{
		Mapper: mapper,
	}
	kclnt, err := client.New(cfg, copts)
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
