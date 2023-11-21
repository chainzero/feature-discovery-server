package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	corev1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	v1 "akash-api/v1"
)

const (
	nvidiaVendorID = "10de"
	pciDevicesDir  = "/sys/bus/pci/devices/"
	jsonURL        = "https://gist.githubusercontent.com/chainzero/279e737f85d71084725ffde7821a9084/raw/c7878123bf8cc8646798fedc80f17ba08494f601/akashGpuDatabase.json"

	gpuResourceName     = "nvidia.com/gpu"
	cpuResourceName     = "cpu"
	memResourceName     = "memory"
	storageResourceName = "ephemeral-storage"
)

var currentNodeData v1.Node

type GPUInfo struct {
	Vendor    string `json:"vendor"`
	Name      string `json:"name"`
	ModelID   string `json:"modelid"`
	Interface string `json:"interface"`
	Memory    string `json:"memory"`
}

type SerializableResourcePair struct {
	Allocatable string `json:"allocatable"`
	Allocated   string `json:"allocated"`
}

type SerializableGPU struct {
	Quantity *SerializableResourcePair `json:"quantity"`
}

func getNodeIntel() (*v1.Node, error) {
	node := &v1.Node{}

	cpu, err := parseCPUInfo()
	if err != nil {
		log.Printf("Error parsing CPU info: %v", err)
		// Depending on your application's logic, handle partial information
		// e.g., return nil, err
	} else {
		node.CPU = *cpu
	}

	resp, err := http.Get(jsonURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching GPU data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("non-200 status code received: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)

	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	var gpuInfos []GPUInfo
	if err := json.Unmarshal(body, &gpuInfos); err != nil {
		return nil, fmt.Errorf("error unmarshaling GPU info: %w", err)
	}

	if err := matchGPUsByDeviceID(node, gpuInfos); err != nil {
		return nil, err
	}

	if err := nodeAllocatableDiscovery(node); err != nil {
		return nil, err
	}

	return node, nil
}

func matchGPUsByDeviceID(node *v1.Node, gpuInfos []GPUInfo) error {
	files, err := os.ReadDir(pciDevicesDir)
	if err != nil {
		return fmt.Errorf("error reading PCI devices directory: %w", err)
	}

	for _, entry := range files {
		fileInfo, err := entry.Info()
		if err != nil {
			// Handle the error, e.g., log it and continue to the next file
			log.Printf("Error getting FileInfo for %s: %v", entry.Name(), err)
			continue
		}

		if err := processFile(fileInfo, node, gpuInfos); err != nil {
			log.Printf("Error processing file %s: %v", fileInfo.Name(), err)
			return err
		}
	}

	return nil
}

func processFile(f os.FileInfo, node *v1.Node, gpuInfos []GPUInfo) error {
	vendorFilePath := filepath.Join(pciDevicesDir, f.Name(), "vendor")
	vendorContent, err := os.ReadFile(vendorFilePath)
	if err != nil {
		return fmt.Errorf("error reading vendor file '%s': %w", vendorFilePath, err)
	}

	if containsVendorID(string(vendorContent), nvidiaVendorID) {
		deviceFilePath := filepath.Join(pciDevicesDir, f.Name(), "device")
		deviceContent, err := os.ReadFile(deviceFilePath)
		if err != nil {
			return fmt.Errorf("error reading device file '%s': %w", deviceFilePath, err)
		}

		deviceID := strings.TrimSpace(string(deviceContent))[2:] // Remove the "0x" prefix and trim spaces

		for _, gpuInfo := range gpuInfos {
			if gpuInfo.ModelID == deviceID {
				gpu := &v1.GPUInfo{
					Vendor:    gpuInfo.Vendor,
					Name:      gpuInfo.Name,
					ModelID:   gpuInfo.ModelID,
					Interface: gpuInfo.Interface,
					Memory:    gpuInfo.Memory,
				}
				node.Gpus.Info = append(node.Gpus.Info, gpu)
				break
			}
		}
	}

	return nil
}

func containsVendorID(content, vendorID string) bool {
	trimmedContent := strings.TrimSpace(content)
	return strings.HasSuffix(trimmedContent, vendorID)
}

func parseCPUInfo() (*v1.CPU, error) {
	file, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return nil, fmt.Errorf("failed to open /proc/cpuinfo: %w", err)
	}
	defer file.Close()

	cpuInfoMap := make(map[string]*v1.CPUInfo)
	scanner := bufio.NewScanner(file)

	var currentCPU *v1.CPUInfo
	for scanner.Scan() {
		line := scanner.Text()
		keyVal := strings.Split(line, ":")
		if len(keyVal) != 2 {
			continue
		}

		key := strings.TrimSpace(keyVal[0])
		value := strings.TrimSpace(keyVal[1])

		switch key {
		case "physical id":
			if _, exists := cpuInfoMap[value]; !exists {
				cpuInfoMap[value] = &v1.CPUInfo{ID: value}
			}
			currentCPU = cpuInfoMap[value]
		case "vendor_id":
			if currentCPU != nil {
				currentCPU.Vendor = value
			}
		case "model name":
			if currentCPU != nil {
				currentCPU.Model = value
			}
		case "cpu cores":
			if currentCPU != nil {
				cores, err := strconv.ParseUint(value, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("failed to parse CPU cores from /proc/cpuinfo: %w", err)
				}
				currentCPU.Vcores = uint32(cores)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading from /proc/cpuinfo: %w", err)
	}

	cpu := &v1.CPU{
		Info: make([]*v1.CPUInfo, 0, len(cpuInfoMap)),
	}
	for _, info := range cpuInfoMap {
		cpu.Info = append(cpu.Info, info)
	}

	return cpu, nil
}

type msgServiceServer struct {
	v1.UnimplementedMsgServer
}

func (s *msgServiceServer) QueryNode(empty *v1.VoidNoParam, stream v1.Msg_QueryNodeServer) error {
	for {
		node, err := getNodeIntel()
		if err != nil {
			log.Printf("Error getting node intelligence: %v", err)
			return err
		}

		node2 := v1.Node{}
		node2.CPU.Info = node.CPU.Info
		node2.Gpus.Info = node.Gpus.Info
		node2.Memory.Info = node.Memory.Info
		node2.Storage.Info = node.Storage.Info

		// CPU Logic to deal with unexported fields in Kubernetes API machinery API > Quantity struct

		// Check if QuantityLocal is nil and initialize if necessary
		if node2.CPU.QuantityLocal == nil {
			node2.CPU.QuantityLocal = &v1.ResourcePairLocal{}
		}

		allocatableStrCpu := node.CPU.Quantity.Allocatable.String()
		node2.CPU.QuantityLocal.Allocatable = allocatableStrCpu
		allocatatedStrCpu := node.CPU.Quantity.Allocated.String()
		node2.CPU.QuantityLocal.Allocated = allocatatedStrCpu

		// GPU Logic to deal with unexported fields in Kubernetes API machinery API > Quantity struct

		// Check if QuantityLocal is nil and initialize if necessary
		if node2.Gpus.QuantityLocal == nil {
			node2.Gpus.QuantityLocal = &v1.ResourcePairLocal{}
		}

		allocatableStrGpu := node.Gpus.Quantity.Allocatable.String()
		node2.Gpus.QuantityLocal.Allocatable = allocatableStrGpu
		allocatatedStrGpu := node.Gpus.Quantity.Allocated.String()
		node2.Gpus.QuantityLocal.Allocated = allocatatedStrGpu

		// Memory Logic to deal with unexported fields in Kubernetes API machinery API > Quantity struct

		// Check if QuantityLocal is nil and initialize if necessary
		if node2.Memory.QuantityLocal == nil {
			node2.Memory.QuantityLocal = &v1.ResourcePairLocal{}
		}

		allocatableStrMem := node.Memory.Quantity.Allocatable.String()
		node2.Memory.QuantityLocal.Allocatable = allocatableStrMem
		allocatatedStrMem := node.Memory.Quantity.Allocated.String()
		node2.Memory.QuantityLocal.Allocated = allocatatedStrMem

		// Storage Logic to deal with unexported fields in Kubernetes API machinery API > Quantity struct

		// Check if QuantityLocal is nil and initialize if necessary
		if node2.Storage.QuantityLocal == nil {
			node2.Storage.QuantityLocal = &v1.ResourcePairLocal{}
		}

		allocatableStrStorage := node.Storage.Quantity.Allocatable.String()
		node2.Storage.QuantityLocal.Allocatable = allocatableStrStorage
		allocatatedStrStorage := node.Storage.Quantity.Allocated.String()
		node2.Storage.QuantityLocal.Allocated = allocatatedStrStorage

		// Check if data collected has updated and only send to gRPC stream if there is an update
		fmt.Println("currentNodeData: ", currentNodeData)
		fmt.Println("node2: ", node2)
		if !reflect.DeepEqual(currentNodeData, node2) {
			currentNodeData = node2 // Update the current data with the new data

			// Send stream to subscribed gRPC clients
			fmt.Printf("Data sent to gRPC server stream: %+v\n", node2)
			if err := stream.Send(&node2); err != nil {
				return err
			}
		} else {
			fmt.Println("No changes detected in node data.")
		}

		// Sleep or wait for some event before sending the next node
		time.Sleep(5 * time.Second)
	}
}

func getAllocatedResourceForNode(clientset *kubernetes.Clientset, nodeName string, resourceName corev1.ResourceName) (int64, error) {
	var totalAllocated int64

	podList, err := clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s,status.phase=Running", nodeName),
	})
	if err != nil {
		return 0, fmt.Errorf("error fetching pods for node %s: %w", nodeName, err)
	}

	for _, pod := range podList.Items {
		for _, container := range pod.Spec.Containers {
			if value, ok := container.Resources.Requests[resourceName]; ok {
				if resourceName == corev1.ResourceCPU {
					totalAllocated += value.MilliValue() // Use MilliValue for CPU resources
				} else {
					allocated, _ := value.AsInt64() // AsInt64 can be used for other resources
					totalAllocated += allocated
				}
			}
		}
	}

	return totalAllocated, nil
}

func nodeAllocatableDiscovery(node *v1.Node) error {
	// Get the node name from the environment variable set by the Downward API
	currentNodeName := os.Getenv("NODE_NAME")
	if currentNodeName == "" {
		return fmt.Errorf("NODE_NAME environment variable not set")
	}

	// Use the in-cluster config. This will fall back to using the service account that is mounted by default by Kubernetes
	// But you need to be aware that this might not work if you are running this outside of Kubernetes
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error obtaining in-cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error creating Kubernetes client: %w", err)
	}

	// Fetch only the details for the current node
	knode, err := clientset.CoreV1().Nodes().Get(context.TODO(), currentNodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error fetching node %s: %w", currentNodeName, err)
	}

	// Extract the allocatable and allocated resources from the current node
	gpuTotal := knode.Status.Capacity[corev1.ResourceName(gpuResourceName)]
	cpuTotal := knode.Status.Capacity[corev1.ResourceName(cpuResourceName)]
	memTotal := knode.Status.Capacity[corev1.ResourceName(memResourceName)]
	storageTotal := knode.Status.Capacity[corev1.ResourceName(storageResourceName)]

	AllocatedGPUs, err := getAllocatedResourceForNode(clientset, knode.Name, corev1.ResourceName(gpuResourceName))
	if err != nil {
		return fmt.Errorf("error getting allocated GPUs: %w", err)
	}
	allocatedCPUs, err := getAllocatedResourceForNode(clientset, knode.Name, corev1.ResourceName(cpuResourceName))
	if err != nil {
		return fmt.Errorf("error getting allocated CPUs: %w", err)
	}

	allocatedMemory, err := getAllocatedResourceForNode(clientset, knode.Name, corev1.ResourceName(memResourceName))
	if err != nil {
		return fmt.Errorf("error getting allocated memory: %w", err)
	}

	allocatedStorage, err := getAllocatedResourceForNode(clientset, knode.Name, corev1.ResourceName(storageResourceName))
	if err != nil {
		return fmt.Errorf("error getting allocated storage: %w", err)
	}

	AllocatedGPUsQuantity := resource.NewQuantity(int64(AllocatedGPUs), resource.DecimalSI)
	allocatedCPUsQuantity := resource.NewQuantity(int64(allocatedCPUs), resource.DecimalSI)
	allocatedMemoryQuantity := resource.NewQuantity(int64(allocatedMemory), resource.DecimalSI)
	allocatedStorageQuantity := resource.NewQuantity(int64(allocatedStorage), resource.DecimalSI)

	// Construct the GPU info
	var gpuInfos []*v1.GPUInfo
	if len(node.Gpus.Info) > 0 {
		gpuInfos = make([]*v1.GPUInfo, len(node.Gpus.Info))
		for i, info := range node.Gpus.Info {
			gpuInfos[i] = &v1.GPUInfo{
				Vendor:    info.Vendor,
				Name:      info.Name,
				ModelID:   info.ModelID,
				Interface: info.Interface,
				Memory:    info.Memory,
			}
		}
	}

	gpus := v1.GPU{
		Quantity: &v1.ResourcePair{
			Allocatable: gpuTotal,
			Allocated:   *AllocatedGPUsQuantity,
		},
		Info: gpuInfos,
	}

	// Construct the CPU info
	var cpuInfos []*v1.CPUInfo
	if len(node.CPU.Info) > 0 {
		cpuInfos = make([]*v1.CPUInfo, len(node.CPU.Info))
		for i, info := range node.CPU.Info {
			cpuInfos[i] = &v1.CPUInfo{
				ID:     info.ID,
				Vendor: info.Vendor,
				Model:  info.Model,
				Vcores: info.Vcores,
			}
		}
		return nil
	}

	cpus := v1.CPU{
		Quantity: &v1.ResourcePair{
			Allocatable: cpuTotal,
			Allocated:   *allocatedCPUsQuantity,
		},
		Info: cpuInfos,
	}

	// Construct the Memory info
	memory := v1.Memory{
		Quantity: &v1.ResourcePair{
			Allocatable: memTotal,
			Allocated:   *allocatedMemoryQuantity,
		},
	}

	// Construct the Storage info
	storage := v1.Storage{
		Quantity: &v1.ResourcePair{
			Allocatable: storageTotal,
			Allocated:   *allocatedStorageQuantity,
		},
	}

	// Now the node structure is safely updated with the new information.
	node.Gpus = gpus
	node.CPU = cpus
	node.Memory = memory
	node.Storage = storage

	return nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	v1.RegisterMsgServer(grpcServer, &msgServiceServer{})
	reflection.Register(grpcServer)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
