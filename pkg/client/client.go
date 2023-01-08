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

package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/google/uuid"
	prismgoclient "github.com/nutanix-cloud-native/prism-go-client"
	"github.com/nutanix-cloud-native/prism-go-client/environment"
	credentialTypes "github.com/nutanix-cloud-native/prism-go-client/environment/credentials"
	kubernetesEnv "github.com/nutanix-cloud-native/prism-go-client/environment/providers/kubernetes"
	envTypes "github.com/nutanix-cloud-native/prism-go-client/environment/types"
	"github.com/nutanix-cloud-native/prism-go-client/utils"
	nutanixClientV3 "github.com/nutanix-cloud-native/prism-go-client/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apitypes "k8s.io/apimachinery/pkg/types"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/klog/v2"
	capiv1 "sigs.k8s.io/cluster-api/api/v1beta1"
	capierrors "sigs.k8s.io/cluster-api/errors"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "github.com/nutanix-cloud-native/cluster-api-provider-nutanix/api/v1beta1"
	nctx "github.com/nutanix-cloud-native/cluster-api-provider-nutanix/pkg/context"
)

var (
	minMachineSystemDiskSize resource.Quantity
	minMachineMemorySize     resource.Quantity
	minVCPUsPerSocket        = 1
	minVCPUSockets           = 1
)

const (
	providerIdPrefix = "nutanix://"
)

func init() {
	minMachineSystemDiskSize = resource.MustParse("20Gi")
	minMachineMemorySize = resource.MustParse("2Gi")
}

const (
	projectKind          = "project"
	defaultEndpointPort  = "9440"
	ProviderName         = "nutanix"
	configPath           = "/etc/nutanix/config"
	endpointKey          = "prismCentral"
	capxNamespaceKey     = "POD_NAMESPACE"
	taskSucceededMessage = "SUCCEEDED"
	serviceNamePECluster = "AOS"
)

var _ PrismClientWrapperInterface = &PrismClientWrapper{}

// PrismClientWrapperInterface defines methods that are used by the controllers.
// Here we can abstract out the methods as verbs where the underlying implementation can be swapped out by a v4 client
// in the future.
type PrismClientWrapperInterface interface {
	GetClientFromEnvironment(*infrav1.NutanixCluster) (*nutanixClientV3.Client, error)
	GetOrCreateVM(*nctx.MachineContext, client.Client) (*nutanixClientV3.VMIntentResponse, error)
}

// PrismClientWrapper wraps the nutanix client and provides helper methods that are used by the controllers.
type PrismClientWrapper struct {
	rwmutex           *sync.RWMutex
	v3ClientMap       map[string]*nutanixClientV3.Client
	secretInformer    coreinformers.SecretInformer
	configMapInformer coreinformers.ConfigMapInformer
}

func NewNutanixClientWrapper(secretInformer coreinformers.SecretInformer, cmInformer coreinformers.ConfigMapInformer) PrismClientWrapperInterface {
	return &PrismClientWrapper{
		secretInformer:    secretInformer,
		configMapInformer: cmInformer,
		rwmutex:           &sync.RWMutex{},
		v3ClientMap:       make(map[string]*nutanixClientV3.Client),
	}
}

func (n *PrismClientWrapper) GetOrCreateVM(rctx *nctx.MachineContext, k8s client.Client) (*nutanixClientV3.VMIntentResponse, error) {
	var err error
	var vm *nutanixClientV3.VMIntentResponse
	ctx := rctx.Context
	vmName := rctx.NutanixMachine.Name
	nc := rctx.NutanixClient

	// Check if the VM already exists
	vm, err = FindVM(ctx, nc, rctx.NutanixMachine)
	if err != nil {
		return nil, err
	}

	if vm != nil {
		// VM exists case
		klog.Infof("%s vm %s found with UUID %s", rctx.LogPrefix, *vm.Spec.Name, rctx.NutanixMachine.Status.VmUUID)
	} else {
		// VM Does not exist case
		klog.Infof("%s No existing VM found. Starting creation process of VM %s.", rctx.LogPrefix, vmName)

		err = n.validateMachineConfig(rctx)
		if err != nil {
			rctx.SetFailureStatus(capierrors.CreateMachineError, err)
			return nil, err
		}

		// Get PE UUID
		peUUID, err := GetPEUUID(ctx, nc, rctx.NutanixMachine.Spec.Cluster.Name, rctx.NutanixMachine.Spec.Cluster.UUID)
		if err != nil {
			errorMsg := fmt.Errorf("failed to get the Prism Element Cluster UUID to create the VM %s. %v", vmName, err)
			rctx.SetFailureStatus(capierrors.CreateMachineError, errorMsg)
			return nil, err
		}

		// Get Subnet UUIDs
		subnetUUIDs, err := GetSubnetUUIDList(ctx, nc, rctx.NutanixMachine.Spec.Subnets, peUUID)
		if err != nil {
			errorMsg := fmt.Errorf("failed to get the subnet UUIDs to create the VM %s. %v", vmName, err)
			rctx.SetFailureStatus(capierrors.CreateMachineError, errorMsg)
			return nil, err
		}

		// Get Image UUID
		imageUUID, err := GetImageUUID(
			ctx,
			nc,
			rctx.NutanixMachine.Spec.Image.Name,
			rctx.NutanixMachine.Spec.Image.UUID,
		)
		if err != nil {
			errorMsg := fmt.Errorf("failed to get the image UUID to create the VM %s. %v", vmName, err)
			klog.Errorf("%s %v", rctx.LogPrefix, errorMsg)
			rctx.SetFailureStatus(capierrors.CreateMachineError, errorMsg)
			return nil, err
		}

		// Get the bootstrapData from the referenced secret
		bootstrapData, err := n.getBootstrapData(rctx, k8s)
		if err != nil {
			return nil, err
		}
		// Encode the bootstrapData by base64
		bsdataEncoded := base64.StdEncoding.EncodeToString(bootstrapData)
		klog.Infof("%s Retrieved the bootstrap data from secret %s (before encoding size: %d, encoded string size:%d)",
			rctx.LogPrefix, rctx.NutanixMachine.Spec.BootstrapRef.Name, len(bootstrapData), len(bsdataEncoded))

		klog.Infof("%s Creating VM with name %s for cluster %s.", rctx.LogPrefix,
			rctx.NutanixMachine.Name, rctx.NutanixCluster.Name)

		vmInput := nutanixClientV3.VMIntentInput{}
		vmSpec := nutanixClientV3.VM{Name: utils.StringPtr(vmName)}

		nicList := make([]*nutanixClientV3.VMNic, 0)
		for _, subnetUUID := range subnetUUIDs {
			nicList = append(nicList, &nutanixClientV3.VMNic{
				SubnetReference: &nutanixClientV3.Reference{
					UUID: utils.StringPtr(subnetUUID),
					Kind: utils.StringPtr("subnet"),
				},
			})
		}

		// Create Disk Spec for systemdisk to be set later in VM Spec
		diskSize := rctx.NutanixMachine.Spec.SystemDiskSize
		diskSizeMib := GetMibValueOfQuantity(diskSize)
		systemDisk, err := CreateSystemDiskSpec(imageUUID, diskSizeMib)
		if err != nil {
			errorMsg := fmt.Errorf("error occurred while creating system disk spec: %v", err)
			rctx.SetFailureStatus(capierrors.CreateMachineError, errorMsg)
			return nil, errorMsg
		}
		diskList := []*nutanixClientV3.VMDisk{
			systemDisk,
		}

		// Set Categories to VM Sepc before creating VM
		categories, err := GetCategoryVMSpec(ctx, nc, n.getMachineCategoryIdentifiers(rctx))
		if err != nil {
			errorMsg := fmt.Errorf("error occurred while creating category spec for vm %s: %v", vmName, err)
			rctx.SetFailureStatus(capierrors.CreateMachineError, errorMsg)
			return nil, errorMsg
		}
		vmMetadata := nutanixClientV3.Metadata{
			Kind:        utils.StringPtr("vm"),
			SpecVersion: utils.Int64Ptr(1),
			Categories:  categories,
		}

		vmMetadataPtr := &vmMetadata

		// Set Project in VM Spec before creating VM
		err = n.addVMToProject(rctx, vmMetadataPtr)
		if err != nil {
			errorMsg := fmt.Errorf("error occurred while trying to add VM %s to project: %v", vmName, err)
			rctx.SetFailureStatus(capierrors.CreateMachineError, errorMsg)
			return nil, err
		}

		memorySize := rctx.NutanixMachine.Spec.MemorySize
		memorySizeMib := GetMibValueOfQuantity(memorySize)

		vmSpec.Resources = &nutanixClientV3.VMResources{
			PowerState:            utils.StringPtr("ON"),
			HardwareClockTimezone: utils.StringPtr("UTC"),
			NumVcpusPerSocket:     utils.Int64Ptr(int64(rctx.NutanixMachine.Spec.VCPUsPerSocket)),
			NumSockets:            utils.Int64Ptr(int64(rctx.NutanixMachine.Spec.VCPUSockets)),
			MemorySizeMib:         utils.Int64Ptr(memorySizeMib),
			NicList:               nicList,
			DiskList:              diskList,
			GuestCustomization: &nutanixClientV3.GuestCustomization{
				IsOverridable: utils.BoolPtr(true),
				CloudInit:     &nutanixClientV3.GuestCustomizationCloudInit{UserData: utils.StringPtr(bsdataEncoded)},
			},
		}
		vmSpec.ClusterReference = &nutanixClientV3.Reference{
			Kind: utils.StringPtr("cluster"),
			UUID: utils.StringPtr(peUUID),
		}
		vmSpecPtr := &vmSpec

		// Set BootType in VM Spec before creating VM
		err = n.addBootTypeToVM(rctx, vmSpecPtr)
		if err != nil {
			errorMsg := fmt.Errorf("error occurred while adding boot type to vm spec: %v", err)
			rctx.SetFailureStatus(capierrors.CreateMachineError, errorMsg)
			return nil, err
		}
		vmInput.Spec = vmSpecPtr
		vmInput.Metadata = vmMetadataPtr

		// Create the actual VM/Machine
		vmResponse, err := nc.V3.CreateVM(ctx, &vmInput)
		if err != nil {
			errorMsg := fmt.Errorf("failed to create VM %s. error: %v", vmName, err)
			rctx.SetFailureStatus(capierrors.CreateMachineError, errorMsg)
			return nil, err
		}
		vmUuid := *vmResponse.Metadata.UUID
		klog.Infof("%s Sent the post request to create VM %s. Got the vm UUID: %s, status.state: %s", rctx.LogPrefix,
			rctx.NutanixMachine.Name, vmUuid, *vmResponse.Status.State)
		klog.Infof("%s Getting task uuid for VM %s", rctx.LogPrefix,
			rctx.NutanixMachine.Name)
		lastTaskUUID, err := GetTaskUUIDFromVM(vmResponse)
		if err != nil {
			errorMsg := fmt.Errorf("%s error occurred fetching task UUID from vm %s after creation: %v", rctx.LogPrefix, rctx.NutanixMachine.Name, err)
			return nil, errorMsg
		}
		klog.Infof("%s Waiting for task %s to get completed for VM %s", rctx.LogPrefix,
			lastTaskUUID, rctx.NutanixMachine.Name)
		err = WaitForTaskCompletion(ctx, nc, lastTaskUUID)
		if err != nil {
			errorMsg := fmt.Errorf("%s  error occurred while waiting for task %s to start: %v", rctx.LogPrefix, lastTaskUUID, err)
			rctx.SetFailureStatus(capierrors.CreateMachineError, errorMsg)
			return nil, errorMsg
		}
		klog.Infof("%s Fetching VM after creation %s", rctx.LogPrefix,
			lastTaskUUID, rctx.NutanixMachine.Name)
		vm, err = FindVMByUUID(ctx, nc, vmUuid)
		if err != nil {
			errorMsg := fmt.Errorf("%s  error occurred while getting VM %s after creation: %v", rctx.LogPrefix, rctx.NutanixMachine.Name, err)
			rctx.SetFailureStatus(capierrors.CreateMachineError, errorMsg)
			return nil, errorMsg
		}
	}
	conditions.MarkTrue(rctx.NutanixMachine, infrav1.VMProvisionedCondition)
	return vm, nil
}

func (n *PrismClientWrapper) addBootTypeToVM(rctx *nctx.MachineContext, vmSpec *nutanixClientV3.VM) error {
	bootType := rctx.NutanixMachine.Spec.BootType
	// Defaults to legacy if boot type is not set.
	if bootType != "" {
		if bootType != string(infrav1.NutanixIdentifierBootTypeLegacy) && bootType != string(infrav1.NutanixIdentifierBootTypeUEFI) {
			errorMsg := fmt.Errorf("%s boot type must be %s or %s but was %s", rctx.LogPrefix, string(infrav1.NutanixIdentifierBootTypeLegacy), string(infrav1.NutanixIdentifierBootTypeUEFI), bootType)
			conditions.MarkFalse(rctx.NutanixMachine, infrav1.VMProvisionedCondition, infrav1.VMBootTypeInvalid, capiv1.ConditionSeverityError, errorMsg.Error())
			return errorMsg
		}

		// Only modify VM spec if boot type is UEFI. Otherwise, assume default Legacy mode
		if bootType == string(infrav1.NutanixIdentifierBootTypeUEFI) {
			vmSpec.Resources.BootConfig = &nutanixClientV3.VMBootConfig{
				BootType: utils.StringPtr(strings.ToUpper(bootType)),
			}
		}
	}

	return nil
}

func (n *PrismClientWrapper) getMachineCategoryIdentifiers(rctx *nctx.MachineContext) []*infrav1.NutanixCategoryIdentifier {
	categoryIdentifiers := GetDefaultCAPICategoryIdentifiers(rctx.Cluster.Name)
	additionalCategories := rctx.NutanixMachine.Spec.AdditionalCategories
	if len(additionalCategories) > 0 {
		for _, at := range additionalCategories {
			additionalCat := at
			categoryIdentifiers = append(categoryIdentifiers, &additionalCat)
		}
	}

	return categoryIdentifiers
}

func (n *PrismClientWrapper) validateMachineConfig(rctx *nctx.MachineContext) error {
	if len(rctx.NutanixMachine.Spec.Subnets) == 0 {
		return fmt.Errorf("atleast one subnet is needed to create the VM %s", rctx.NutanixMachine.Name)
	}

	diskSize := rctx.NutanixMachine.Spec.SystemDiskSize
	// Validate disk size
	if diskSize.Cmp(minMachineSystemDiskSize) < 0 {
		diskSizeMib := GetMibValueOfQuantity(diskSize)
		minMachineSystemDiskSizeMib := GetMibValueOfQuantity(minMachineSystemDiskSize)
		return fmt.Errorf("minimum systemDiskSize is %vMib but given %vMib", minMachineSystemDiskSizeMib, diskSizeMib)
	}

	memorySize := rctx.NutanixMachine.Spec.MemorySize
	// Validate memory size
	if memorySize.Cmp(minMachineMemorySize) < 0 {
		memorySizeMib := GetMibValueOfQuantity(memorySize)
		minMachineMemorySizeMib := GetMibValueOfQuantity(minMachineMemorySize)
		return fmt.Errorf("minimum memorySize is %vMib but given %vMib", minMachineMemorySizeMib, memorySizeMib)
	}

	vcpusPerSocket := rctx.NutanixMachine.Spec.VCPUsPerSocket
	if vcpusPerSocket < int32(minVCPUsPerSocket) {
		return fmt.Errorf("minimum vcpus per socket is %v but given %v", minVCPUsPerSocket, vcpusPerSocket)
	}

	vcpuSockets := rctx.NutanixMachine.Spec.VCPUSockets
	if vcpuSockets < int32(minVCPUSockets) {
		return fmt.Errorf("minimum vcpu sockets is %v but given %v", minVCPUSockets, vcpuSockets)
	}

	return nil
}

// getBootstrapData returns the Bootstrap data from the ref secret
func (n *PrismClientWrapper) getBootstrapData(rctx *nctx.MachineContext, k8s client.Client) ([]byte, error) {
	if rctx.NutanixMachine.Spec.BootstrapRef == nil {
		return nil, errors.New("the NutanixMachine spec.BootstrapRef is nil")
	}

	secretName := rctx.NutanixMachine.Spec.BootstrapRef.Name
	secret := &corev1.Secret{}
	secretKey := apitypes.NamespacedName{
		Namespace: rctx.NutanixMachine.Spec.BootstrapRef.Namespace,
		Name:      secretName,
	}
	if err := k8s.Get(rctx.Context, secretKey, secret); err != nil {
		return nil, fmt.Errorf("failed to retrieve bootstrap data secret %s : %w", secretName, err)
	}

	value, ok := secret.Data["value"]
	if !ok {
		return nil, errors.New("error retrieving bootstrap data: secret value key is missing")
	}
	klog.V(6).Infof("%s Retrieved the NutanixMachine bootstrap data (size: %d):\n%s", rctx.LogPrefix, len(value), string(value))

	return value, nil
}

// DeleteVM deletes a VM and is invoked by the NutanixMachineReconciler
func DeleteVM(ctx context.Context, client *nutanixClientV3.Client, vmName, vmUUID string) (string, error) {
	if vmUUID == "" {
		klog.Warning("VmUUID was empty. Skipping delete")
		return "", nil
	}

	klog.Infof("Deleting VM %s with UUID: %s", vmName, vmUUID)
	vmDeleteResponse, err := client.V3.DeleteVM(ctx, vmUUID)
	if err != nil {
		return "", err
	}
	deleteTaskUUID := vmDeleteResponse.Status.ExecutionContext.TaskUUID.(string)

	return deleteTaskUUID, nil
}

// FindVMByUUID retrieves the VM with the given vm UUID. Returns nil if not found
func FindVMByUUID(ctx context.Context, client *nutanixClientV3.Client, uuid string) (*nutanixClientV3.VMIntentResponse, error) {
	klog.Infof("Checking if VM with UUID %s exists.", uuid)

	response, err := client.V3.GetVM(ctx, uuid)
	if err != nil {
		if strings.Contains(fmt.Sprint(err), "ENTITY_NOT_FOUND") {
			return nil, nil
		} else {
			return nil, err
		}
	}

	return response, nil
}

// GetVMUUID returns the UUID of the VM with the given name
func GetVMUUID(nutanixMachine *infrav1.NutanixMachine) (string, error) {
	vmUUID := nutanixMachine.Status.VmUUID
	if vmUUID != "" {
		if _, err := uuid.Parse(vmUUID); err != nil {
			return "", fmt.Errorf("VMUUID was set but was not a valid UUID: %s err: %v", vmUUID, err)
		}
		return vmUUID, nil
	}
	providerID := nutanixMachine.Spec.ProviderID
	if providerID == "" {
		return "", nil
	}
	id := strings.TrimPrefix(providerID, providerIdPrefix)
	// Not returning error since the ProviderID initially is not a UUID. CAPX only sets the UUID after VM provisioning.
	// If it is not a UUID, continue.
	if _, err := uuid.Parse(id); err != nil {
		return "", nil
	}
	return id, nil
}

// GenerateProviderID generates a provider ID for the given resource UUID
func GenerateProviderID(uuid string) string {
	return fmt.Sprintf("%s%s", providerIdPrefix, uuid)
}

func CreateSystemDiskSpec(imageUUID string, systemDiskSize int64) (*nutanixClientV3.VMDisk, error) {
	if imageUUID == "" {
		return nil, fmt.Errorf("image UUID must be set when creating system disk")
	}
	if systemDiskSize <= 0 {
		return nil, fmt.Errorf("invalid system disk size: %d. Provide in XXGi (for example 70Gi) format instead", systemDiskSize)
	}
	systemDisk := &nutanixClientV3.VMDisk{
		DataSourceReference: &nutanixClientV3.Reference{
			Kind: utils.StringPtr("image"),
			UUID: utils.StringPtr(imageUUID),
		},
		DiskSizeMib: utils.Int64Ptr(systemDiskSize),
	}
	return systemDisk, nil
}

// FindVM retrieves the VM with the given uuid or name
func FindVM(ctx context.Context, client *nutanixClientV3.Client, nutanixMachine *infrav1.NutanixMachine) (*nutanixClientV3.VMIntentResponse, error) {
	vmName := nutanixMachine.Name
	vmUUID, err := GetVMUUID(nutanixMachine)
	if err != nil {
		return nil, err
	}
	// Search via uuid if it is present
	if vmUUID != "" {
		klog.Infof("Searching for VM %s using UUID %s", vmName, vmUUID)
		vm, err := FindVMByUUID(ctx, client, vmUUID)
		if err != nil {
			return nil, err
		}
		if vm == nil {
			errorMsg := fmt.Sprintf("no vm %s found with UUID %s but was expected to be present", vmName, vmUUID)
			klog.Error(errorMsg)
			return nil, fmt.Errorf(errorMsg)
		}
		if *vm.Spec.Name != vmName {
			errorMsg := fmt.Sprintf("found VM with UUID %s but name did not match %s. error: %v", vmUUID, vmName, err)
			klog.Errorf(errorMsg)
			return nil, fmt.Errorf(errorMsg)
		}
		return vm, nil
		// otherwise search via name
	} else {
		klog.Infof("Searching for VM %s using name", vmName)
		vm, err := FindVMByName(ctx, client, vmName)
		if err != nil {
			return nil, err
		}
		return vm, nil
	}
}

// FindVMByName retrieves the VM with the given vm name
func FindVMByName(ctx context.Context, client *nutanixClientV3.Client, vmName string) (*nutanixClientV3.VMIntentResponse, error) {
	klog.Infof("Checking if VM with name %s exists.", vmName)

	res, err := client.V3.ListVM(ctx, &nutanixClientV3.DSMetadata{
		Filter: utils.StringPtr(fmt.Sprintf("vm_name==%s", vmName)),
	})
	if err != nil {
		errorMsg := fmt.Errorf("error occurred when searching for VM by name %s. error: %v", vmName, err)
		return nil, errorMsg
	}

	if len(res.Entities) > 1 {
		errorMsg := fmt.Sprintf("Found more than one (%v) vms with name %s.", len(res.Entities), vmName)
		return nil, fmt.Errorf(errorMsg)
	}

	if len(res.Entities) == 0 {
		return nil, nil
	}

	return FindVMByUUID(ctx, client, *res.Entities[0].Metadata.UUID)
}

// GetPEUUID returns the UUID of the Prism Element cluster with the given name
func GetPEUUID(ctx context.Context, client *nutanixClientV3.Client, peName, peUUID *string) (string, error) {
	if peUUID == nil && peName == nil {
		return "", fmt.Errorf("cluster name or uuid must be passed in order to retrieve the pe")
	}
	if peUUID != nil && *peUUID != "" {
		peIntentResponse, err := client.V3.GetCluster(ctx, *peUUID)
		if err != nil {
			if strings.Contains(fmt.Sprint(err), "ENTITY_NOT_FOUND") {
				return "", fmt.Errorf("failed to find Prism Element cluster with UUID %s: %v", *peUUID, err)
			}
		}
		return *peIntentResponse.Metadata.UUID, nil
	} else if peName != nil && *peName != "" {
		filter := getFilterForName(*peName)
		responsePEs, err := client.V3.ListAllCluster(ctx, filter)
		if err != nil {
			return "", err
		}
		// Validate filtered PEs
		foundPEs := make([]*nutanixClientV3.ClusterIntentResponse, 0)
		for _, s := range responsePEs.Entities {
			peSpec := s.Spec
			if *peSpec.Name == *peName && hasPEClusterServiceEnabled(s, serviceNamePECluster) {
				foundPEs = append(foundPEs, s)
			}
		}
		if len(foundPEs) == 1 {
			return *foundPEs[0].Metadata.UUID, nil
		}
		if len(foundPEs) == 0 {
			return "", fmt.Errorf("failed to retrieve Prism Element cluster by name %s", *peName)
		} else {
			return "", fmt.Errorf("more than one Prism Element cluster found with name %s", *peName)
		}
	}
	return "", fmt.Errorf("failed to retrieve Prism Element cluster by name or uuid. Verify input parameters")
}

func (n *PrismClientWrapper) GetClientFromEnvironment(nutanixCluster *infrav1.NutanixCluster) (*nutanixClientV3.Client, error) {
	// Create a list of env providers
	providers := make([]envTypes.Provider, 0)

	// If PrismCentral is set, add the required env provider
	prismCentralInfo := nutanixCluster.Spec.PrismCentral
	if prismCentralInfo != nil {
		if prismCentralInfo.Address == "" {
			return nil, fmt.Errorf("cannot get credentials if Prism Address is not set")
		}
		if prismCentralInfo.Port == 0 {
			return nil, fmt.Errorf("cannot get credentials if Prism Port is not set")
		}
		credentialRef := prismCentralInfo.CredentialRef
		if credentialRef == nil {
			return nil, fmt.Errorf("credentialRef must be set on prismCentral attribute for cluster %s in namespace %s", nutanixCluster.Name, nutanixCluster.Namespace)
		}
		// If namespace is empty, use the cluster namespace
		if credentialRef.Namespace == "" {
			credentialRef.Namespace = nutanixCluster.Namespace
		}
		additionalTrustBundleRef := prismCentralInfo.AdditionalTrustBundle
		if additionalTrustBundleRef != nil &&
			additionalTrustBundleRef.Kind == credentialTypes.NutanixTrustBundleKindConfigMap &&
			additionalTrustBundleRef.Namespace == "" {
			additionalTrustBundleRef.Namespace = nutanixCluster.Namespace
		}
		providers = append(providers, kubernetesEnv.NewProvider(
			*nutanixCluster.Spec.PrismCentral,
			n.secretInformer,
			n.configMapInformer))
	} else {
		klog.Warningf("[WARNING] prismCentral attribute was not set on NutanixCluster %s in namespace %s. Defaulting to CAPX manager credentials", nutanixCluster.Name, nutanixCluster.Namespace)
	}

	// Add env provider for CAPX manager
	npe, err := n.getManagerNutanixPrismEndpoint()
	if err != nil {
		return nil, err
	}
	// If namespaces is not set, set it to the namespace of the CAPX manager
	if npe.CredentialRef.Namespace == "" {
		capxNamespace := os.Getenv(capxNamespaceKey)
		if capxNamespace == "" {
			return nil, fmt.Errorf("failed to retrieve capx-namespace. Make sure %s env variable is set", capxNamespaceKey)
		}
		npe.CredentialRef.Namespace = capxNamespace
	}
	if npe.AdditionalTrustBundle != nil && npe.AdditionalTrustBundle.Namespace == "" {
		capxNamespace := os.Getenv(capxNamespaceKey)
		if capxNamespace == "" {
			return nil, fmt.Errorf("failed to retrieve capx-namespace. Make sure %s env variable is set", capxNamespaceKey)
		}
		npe.AdditionalTrustBundle.Namespace = capxNamespace
	}
	providers = append(providers, kubernetesEnv.NewProvider(
		*npe,
		n.secretInformer,
		n.configMapInformer))

	// init env with providers
	env := environment.NewEnvironment(providers...)
	// fetch endpoint details
	me, err := env.GetManagementEndpoint(envTypes.Topology{})
	if err != nil {
		return nil, err
	}

	creds := prismgoclient.Credentials{
		URL:      me.Address.Host,
		Endpoint: me.Address.Host,
		Insecure: me.Insecure,
		Username: me.ApiCredentials.Username,
		Password: me.ApiCredentials.Password,
	}
	return GetClient(creds, me.AdditionalTrustBundle)
}

func (n *PrismClientWrapper) getManagerNutanixPrismEndpoint() (*credentialTypes.NutanixPrismEndpoint, error) {
	npe := &credentialTypes.NutanixPrismEndpoint{}
	config, err := readEndpointConfig()
	if err != nil {
		return npe, err
	}
	if err = json.Unmarshal(config, npe); err != nil {
		return npe, err
	}
	if npe.CredentialRef == nil {
		return nil, fmt.Errorf("credentialRef must be set on CAPX manager")
	}
	return npe, nil
}

func (n *PrismClientWrapper) addVMToProject(rctx *nctx.MachineContext, vmMetadata *nutanixClientV3.Metadata) error {
	vmName := rctx.NutanixMachine.Name
	projectRef := rctx.NutanixMachine.Spec.Project
	if projectRef == nil {
		klog.Infof("%s Not linking VM %s to a project", rctx.LogPrefix, vmName)
		return nil
	}
	if vmMetadata == nil {
		errorMsg := fmt.Errorf("%s metadata cannot be nil when adding VM %s to project", rctx.LogPrefix, vmName)
		conditions.MarkFalse(rctx.NutanixMachine, infrav1.ProjectAssignedCondition, infrav1.ProjectAssignationFailed, capiv1.ConditionSeverityError, errorMsg.Error())
		return errorMsg
	}
	projectUUID, err := GetProjectUUID(rctx.Context, rctx.NutanixClient, projectRef.Name, projectRef.UUID)
	if err != nil {
		errorMsg := fmt.Errorf("%s error occurred while searching for project for VM %s: %v", rctx.LogPrefix, vmName, err)
		conditions.MarkFalse(rctx.NutanixMachine, infrav1.ProjectAssignedCondition, infrav1.ProjectAssignationFailed, capiv1.ConditionSeverityError, errorMsg.Error())
		return errorMsg
	}
	vmMetadata.ProjectReference = &nutanixClientV3.Reference{
		Kind: utils.StringPtr(projectKind),
		UUID: utils.StringPtr(projectUUID),
	}
	conditions.MarkTrue(rctx.NutanixMachine, infrav1.ProjectAssignedCondition)
	return nil
}

func readEndpointConfig() ([]byte, error) {
	if b, err := os.ReadFile(filepath.Join(configPath, endpointKey)); err == nil {
		return b, err
	} else if os.IsNotExist(err) {
		return []byte{}, nil
	} else {
		return []byte{}, err
	}
}

func GetClient(cred prismgoclient.Credentials, additionalTrustBundle string) (*nutanixClientV3.Client, error) {
	if cred.Username == "" {
		errorMsg := fmt.Errorf("could not create client because username was not set")
		return nil, errorMsg
	}
	if cred.Password == "" {
		errorMsg := fmt.Errorf("could not create client because password was not set")
		return nil, errorMsg
	}
	if cred.Port == "" {
		cred.Port = defaultEndpointPort
	}
	if cred.URL == "" {
		cred.URL = fmt.Sprintf("%s:%s", cred.Endpoint, cred.Port)
	}
	clientOpts := make([]nutanixClientV3.ClientOption, 0)
	if additionalTrustBundle != "" {
		clientOpts = append(clientOpts, nutanixClientV3.WithPEMEncodedCertBundle([]byte(additionalTrustBundle)))
	}
	cli, err := nutanixClientV3.NewV3Client(cred, clientOpts...)
	if err != nil {
		return nil, err
	}

	// Check if the client is working
	_, err = cli.V3.GetCurrentLoggedInUser(context.Background())
	if err != nil {
		return nil, err
	}

	return cli, nil
}

func GetCredentialRefForCluster(nutanixCluster *infrav1.NutanixCluster) (*credentialTypes.NutanixCredentialReference, error) {
	if nutanixCluster == nil {
		return nil, fmt.Errorf("cannot get credential reference if nutanix cluster object is nil")
	}
	prismCentralinfo := nutanixCluster.Spec.PrismCentral
	if prismCentralinfo == nil {
		return nil, nil
	}
	if prismCentralinfo.CredentialRef == nil {
		return nil, fmt.Errorf("credentialRef must be set on prismCentral attribute for cluster %s in namespace %s", nutanixCluster.Name, nutanixCluster.Namespace)
	}
	if prismCentralinfo.CredentialRef.Kind != credentialTypes.SecretKind {
		return nil, nil
	}

	return prismCentralinfo.CredentialRef, nil
}

// GetSubnetUUID returns the UUID of the subnet with the given name
func GetSubnetUUID(ctx context.Context, client *nutanixClientV3.Client, peUUID string, subnetName, subnetUUID *string) (string, error) {
	var foundSubnetUUID string
	if subnetUUID == nil && subnetName == nil {
		return "", fmt.Errorf("subnet name or subnet uuid must be passed in order to retrieve the subnet")
	}
	if subnetUUID != nil {
		subnetIntentResponse, err := client.V3.GetSubnet(ctx, *subnetUUID)
		if err != nil {
			if strings.Contains(fmt.Sprint(err), "ENTITY_NOT_FOUND") {
				return "", fmt.Errorf("failed to find subnet with UUID %s: %v", *subnetUUID, err)
			}
		}
		foundSubnetUUID = *subnetIntentResponse.Metadata.UUID
	} else if subnetName != nil {
		filter := getFilterForName(*subnetName)
		subnetClientSideFilter := getSubnetClientSideFilter(peUUID)
		responseSubnets, err := client.V3.ListAllSubnet(ctx, filter, subnetClientSideFilter)
		if err != nil {
			return "", err
		}
		// Validate filtered Subnets
		foundSubnets := make([]*nutanixClientV3.SubnetIntentResponse, 0)
		for _, s := range responseSubnets.Entities {
			subnetSpec := s.Spec
			if *subnetSpec.Name == *subnetName && *subnetSpec.ClusterReference.UUID == peUUID {
				foundSubnets = append(foundSubnets, s)
			}
		}
		if len(foundSubnets) == 0 {
			return "", fmt.Errorf("failed to retrieve subnet by name %s", *subnetName)
		} else if len(foundSubnets) > 1 {
			return "", fmt.Errorf("more than one subnet found with name %s", *subnetName)
		} else {
			foundSubnetUUID = *foundSubnets[0].Metadata.UUID
		}
		if foundSubnetUUID == "" {
			return "", fmt.Errorf("failed to retrieve subnet by name or uuid. Verify input parameters")
		}
	}
	return foundSubnetUUID, nil
}

// GetImageUUID returns the UUID of the image with the given name
func GetImageUUID(ctx context.Context, client *nutanixClientV3.Client, imageName, imageUUID *string) (string, error) {
	var foundImageUUID string

	if imageUUID == nil && imageName == nil {
		return "", fmt.Errorf("image name or image uuid must be passed in order to retrieve the image")
	}
	if imageUUID != nil {
		imageIntentResponse, err := client.V3.GetImage(ctx, *imageUUID)
		if err != nil {
			if strings.Contains(fmt.Sprint(err), "ENTITY_NOT_FOUND") {
				return "", fmt.Errorf("failed to find image with UUID %s: %v", *imageUUID, err)
			}
		}
		foundImageUUID = *imageIntentResponse.Metadata.UUID
	} else if imageName != nil {
		filter := getFilterForName(*imageName)
		responseImages, err := client.V3.ListAllImage(ctx, filter)
		if err != nil {
			return "", err
		}
		// Validate filtered Images
		foundImages := make([]*nutanixClientV3.ImageIntentResponse, 0)
		for _, s := range responseImages.Entities {
			imageSpec := s.Spec
			if *imageSpec.Name == *imageName {
				foundImages = append(foundImages, s)
			}
		}
		if len(foundImages) == 0 {
			return "", fmt.Errorf("failed to retrieve image by name %s", *imageName)
		} else if len(foundImages) > 1 {
			return "", fmt.Errorf("more than one image found with name %s", *imageName)
		} else {
			foundImageUUID = *foundImages[0].Metadata.UUID
		}
		if foundImageUUID == "" {
			return "", fmt.Errorf("failed to retrieve image by name or uuid. Verify input parameters")
		}
	}
	return foundImageUUID, nil
}

// HasTaskInProgress returns true if the given task is in progress
func HasTaskInProgress(ctx context.Context, client *nutanixClientV3.Client, taskUUID string) (bool, error) {
	taskStatus, err := GetTaskState(ctx, client, taskUUID)
	if err != nil {
		return false, err
	}
	if taskStatus != taskSucceededMessage {
		klog.Infof("VM task with UUID %s still in progress: %s. Requeuing", taskUUID, taskStatus)
		return true, nil
	}
	return false, nil
}

// GetTaskUUIDFromVM returns the UUID of the task that created the VM with the given UUID
func GetTaskUUIDFromVM(vm *nutanixClientV3.VMIntentResponse) (string, error) {
	if vm == nil {
		return "", fmt.Errorf("cannot extract task uuid from empty vm object")
	}
	taskInterface := vm.Status.ExecutionContext.TaskUUID
	vmName := *vm.Spec.Name

	switch t := reflect.TypeOf(taskInterface).Kind(); t {
	case reflect.Slice:
		l := taskInterface.([]interface{})
		if len(l) != 1 {
			return "", fmt.Errorf("did not find expected amount of task UUIDs for VM %s", vmName)
		}
		return l[0].(string), nil
	case reflect.String:
		return taskInterface.(string), nil
	default:
		return "", fmt.Errorf("invalid type found for task uuid extracted from vm %s: %v", vmName, t)
	}
}

// GetSubnetUUIDList returns a list of subnet UUIDs for the given list of subnet names
func GetSubnetUUIDList(ctx context.Context, client *nutanixClientV3.Client, machineSubnets []infrav1.NutanixResourceIdentifier, peUUID string) ([]string, error) {
	subnetUUIDs := make([]string, 0)
	for _, machineSubnet := range machineSubnets {
		subnetUUID, err := GetSubnetUUID(
			ctx,
			client,
			peUUID,
			machineSubnet.Name,
			machineSubnet.UUID,
		)
		if err != nil {
			return subnetUUIDs, err
		}
		subnetUUIDs = append(subnetUUIDs, subnetUUID)
	}
	return subnetUUIDs, nil
}

// GetDefaultCAPICategoryIdentifiers returns the default CAPI category identifiers
func GetDefaultCAPICategoryIdentifiers(clusterName string) []*infrav1.NutanixCategoryIdentifier {
	return []*infrav1.NutanixCategoryIdentifier{
		{
			Key:   fmt.Sprintf("%s%s", infrav1.DefaultCAPICategoryPrefix, clusterName),
			Value: infrav1.DefaultCAPICategoryOwnedValue,
		},
	}
}

// GetOrCreateCategories returns the list of category UUIDs for the given list of category names
func GetOrCreateCategories(ctx context.Context, client *nutanixClientV3.Client, categoryIdentifiers []*infrav1.NutanixCategoryIdentifier) ([]*nutanixClientV3.CategoryValueStatus, error) {
	categories := make([]*nutanixClientV3.CategoryValueStatus, 0)
	for _, ci := range categoryIdentifiers {
		if ci == nil {
			return categories, fmt.Errorf("cannot get or create nil category")
		}
		category, err := getOrCreateCategory(ctx, client, ci)
		if err != nil {
			return categories, err
		}
		categories = append(categories, category)
	}
	return categories, nil
}

func getCategoryKey(ctx context.Context, client *nutanixClientV3.Client, key string) (*nutanixClientV3.CategoryKeyStatus, error) {
	categoryKey, err := client.V3.GetCategoryKey(ctx, key)
	if err != nil {
		if !strings.Contains(fmt.Sprint(err), "ENTITY_NOT_FOUND") {
			errorMsg := fmt.Errorf("failed to retrieve category with key %s. error: %v", key, err)
			return nil, errorMsg
		} else {
			return nil, nil
		}
	}
	return categoryKey, nil
}

func getCategoryValue(ctx context.Context, client *nutanixClientV3.Client, key, value string) (*nutanixClientV3.CategoryValueStatus, error) {
	categoryValue, err := client.V3.GetCategoryValue(ctx, key, value)
	if err != nil {
		if !strings.Contains(fmt.Sprint(err), "CATEGORY_NAME_VALUE_MISMATCH") {
			errorMsg := fmt.Errorf("failed to retrieve category value %s in category %s. error: %v", value, key, err)
			return nil, errorMsg
		} else {
			return nil, nil
		}
	}
	return categoryValue, nil
}

// DeleteCategories deletes the given list of categories
func DeleteCategories(ctx context.Context, client *nutanixClientV3.Client, categoryIdentifiers []*infrav1.NutanixCategoryIdentifier) error {
	groupCategoriesByKey := make(map[string][]string, 0)
	for _, ci := range categoryIdentifiers {
		ciKey := ci.Key
		ciValue := ci.Value
		if gck, ok := groupCategoriesByKey[ciKey]; ok {
			groupCategoriesByKey[ciKey] = append(gck, ciValue)
		} else {
			groupCategoriesByKey[ciKey] = []string{ciValue}
		}
	}

	for key, values := range groupCategoriesByKey {
		klog.Infof("Retrieving category with key %s", key)
		categoryKey, err := getCategoryKey(ctx, client, key)
		if err != nil {
			errorMsg := fmt.Errorf("failed to retrieve category with key %s. error: %v", key, err)
			klog.Error(errorMsg)
			return errorMsg
		}
		klog.Infof("Category with key %s found. Starting deletion of values", key)
		if categoryKey == nil {
			klog.Infof("Category with key %s not found. Already deleted?", key)
			continue
		}
		for _, value := range values {
			categoryValue, err := getCategoryValue(ctx, client, key, value)
			if err != nil {
				errorMsg := fmt.Errorf("failed to retrieve category value %s in category %s. error: %v", value, key, err)
				return errorMsg
			}
			if categoryValue == nil {
				klog.Infof("Category with value %s in category %s not found. Already deleted?", value, key)
				continue
			}

			err = client.V3.DeleteCategoryValue(ctx, key, value)
			if err != nil {
				errorMsg := fmt.Errorf("failed to delete category with key %s. error: %v", key, err)
				return errorMsg
			}
		}
		// check if there are remaining category values
		categoryKeyValues, err := client.V3.ListCategoryValues(ctx, key, &nutanixClientV3.CategoryListMetadata{})
		if err != nil {
			errorMsg := fmt.Errorf("failed to get values of category with key %s: %v", key, err)
			return errorMsg
		}
		if len(categoryKeyValues.Entities) > 0 {
			errorMsg := fmt.Errorf("cannot remove category with key %s because it still has category values assigned", key)
			return errorMsg
		}
		klog.Infof("No values assigned to category. Removing category with key %s", key)
		err = client.V3.DeleteCategoryKey(ctx, key)
		if err != nil {
			errorMsg := fmt.Errorf("failed to delete category with key %s: %v", key, err)
			return errorMsg
		}
	}
	return nil
}

func getOrCreateCategory(ctx context.Context, client *nutanixClientV3.Client, categoryIdentifier *infrav1.NutanixCategoryIdentifier) (*nutanixClientV3.CategoryValueStatus, error) {
	if categoryIdentifier == nil {
		return nil, fmt.Errorf("category identifier cannot be nil when getting or creating categories")
	}
	if categoryIdentifier.Key == "" {
		return nil, fmt.Errorf("category identifier key must be set when when getting or creating categories")
	}
	if categoryIdentifier.Value == "" {
		return nil, fmt.Errorf("category identifier key must be set when when getting or creating categories")
	}
	klog.Infof("Checking existence of category with key %s", categoryIdentifier.Key)
	categoryKey, err := getCategoryKey(ctx, client, categoryIdentifier.Key)
	if err != nil {
		errorMsg := fmt.Errorf("failed to retrieve category with key %s. error: %v", categoryIdentifier.Key, err)
		return nil, errorMsg
	}
	if categoryKey == nil {
		klog.Infof("Category with key %s did not exist.", categoryIdentifier.Key)
		categoryKey, err = client.V3.CreateOrUpdateCategoryKey(ctx, &nutanixClientV3.CategoryKey{
			Description: utils.StringPtr(infrav1.DefaultCAPICategoryDescription),
			Name:        utils.StringPtr(categoryIdentifier.Key),
		})
		if err != nil {
			errorMsg := fmt.Errorf("failed to create category with key %s. error: %v", categoryIdentifier.Key, err)
			return nil, errorMsg
		}
	}
	categoryValue, err := getCategoryValue(ctx, client, *categoryKey.Name, categoryIdentifier.Value)
	if err != nil {
		errorMsg := fmt.Errorf("failed to retrieve category value %s in category %s. error: %v", categoryIdentifier.Value, categoryIdentifier.Key, err)
		return nil, errorMsg
	}
	if categoryValue == nil {
		categoryValue, err = client.V3.CreateOrUpdateCategoryValue(ctx, *categoryKey.Name, &nutanixClientV3.CategoryValue{
			Description: utils.StringPtr(infrav1.DefaultCAPICategoryDescription),
			Value:       utils.StringPtr(categoryIdentifier.Value),
		})
		if err != nil {
			klog.Errorf("Failed to create category value %s in category key %s. error: %v", categoryIdentifier.Value, categoryIdentifier.Key, err)
		}
	}
	return categoryValue, nil
}

// GetCategoryVMSpec returns a flatmap of categories and their values
func GetCategoryVMSpec(ctx context.Context, client *nutanixClientV3.Client, categoryIdentifiers []*infrav1.NutanixCategoryIdentifier) (map[string]string, error) {
	categorySpec := map[string]string{}
	for _, ci := range categoryIdentifiers {
		categoryValue, err := getCategoryValue(ctx, client, ci.Key, ci.Value)
		if err != nil {
			errorMsg := fmt.Errorf("error occurred while to retrieving category value %s in category %s. error: %v", ci.Value, ci.Key, err)
			return nil, errorMsg
		}
		if categoryValue == nil {
			errorMsg := fmt.Errorf("category value %s not found in category %s. error", ci.Value, ci.Key)
			return nil, errorMsg
		}
		categorySpec[ci.Key] = ci.Value
	}
	return categorySpec, nil
}

// GetProjectUUID returns the UUID of the project with the given name
func GetProjectUUID(ctx context.Context, client *nutanixClientV3.Client, projectName, projectUUID *string) (string, error) {
	var foundProjectUUID string
	if projectUUID == nil && projectName == nil {
		return "", fmt.Errorf("name or uuid must be passed in order to retrieve the project")
	}
	if projectUUID != nil {
		projectIntentResponse, err := client.V3.GetProject(ctx, *projectUUID)
		if err != nil {
			if strings.Contains(fmt.Sprint(err), "ENTITY_NOT_FOUND") {
				return "", fmt.Errorf("failed to find project with UUID %s: %v", *projectUUID, err)
			}
		}
		foundProjectUUID = *projectIntentResponse.Metadata.UUID
	} else if projectName != nil {
		filter := getFilterForName(*projectName)
		responseProjects, err := client.V3.ListAllProject(ctx, filter)
		if err != nil {
			return "", err
		}
		foundProjects := make([]*nutanixClientV3.Project, 0)
		for _, s := range responseProjects.Entities {
			projectSpec := s.Spec
			if projectSpec.Name == *projectName {
				foundProjects = append(foundProjects, s)
			}
		}
		if len(foundProjects) == 0 {
			return "", fmt.Errorf("failed to retrieve project by name %s", *projectName)
		} else if len(foundProjects) > 1 {
			return "", fmt.Errorf("more than one project found with name %s", *projectName)
		} else {
			foundProjectUUID = *foundProjects[0].Metadata.UUID
		}
		if foundProjectUUID == "" {
			return "", fmt.Errorf("failed to retrieve project by name or uuid. Verify input parameters")
		}
	}
	return foundProjectUUID, nil
}

func getSubnetClientSideFilter(peUUID string) []*prismgoclient.AdditionalFilter {
	clientSideFilters := make([]*prismgoclient.AdditionalFilter, 0)
	return append(clientSideFilters, &prismgoclient.AdditionalFilter{
		Name: "cluster_reference.uuid",
		Values: []string{
			peUUID,
		},
	})
}

func getFilterForName(name string) string {
	return fmt.Sprintf("name==%s", name)
}

func hasPEClusterServiceEnabled(peCluster *nutanixClientV3.ClusterIntentResponse, serviceName string) bool {
	if peCluster.Status == nil ||
		peCluster.Status.Resources == nil ||
		peCluster.Status.Resources.Config == nil {
		return false
	}
	serviceList := peCluster.Status.Resources.Config.ServiceList
	for _, s := range serviceList {
		if s != nil && strings.ToUpper(*s) == serviceName {
			return true
		}
	}
	return false
}

// GetMibValueOfQuantity returns the given quantity value in Mib
func GetMibValueOfQuantity(quantity resource.Quantity) int64 {
	return quantity.Value() / (1024 * 1024)
}
