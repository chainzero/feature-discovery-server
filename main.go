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

	v1 "github.com/akash-network/akash-api/go/inventory/v1"
)

const (
	nvidiaVendorID      = "10de"
	pciDevicesDir       = "/sys/bus/pci/devices/"
	gistID              = "279e737f85d71084725ffde7821a9084"
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

	// Context for parseCPUInfo with a 500ms timeout
	ctxCPU, cancelCPU := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelCPU()

	cpu, err := parseCPUInfo(ctxCPU)
	if err != nil {
		log.Printf("Error parsing CPU info: %v", err)
		return nil, err
	}
	node.CPU = *cpu

	// Separate context for the HTTP request with a 2-second timeout
	ctxHTTP, cancelHTTP := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelHTTP()

	// GitHub API URL for the Gist
	gistAPIURL := fmt.Sprintf("https://api.github.com/gists/%s", gistID)

	// Create a new HTTP request with context for the GitHub API
	req, err := http.NewRequestWithContext(ctxHTTP, "GET", gistAPIURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request for GitHub API: %w", err)
	}

	// Use http.Client to do the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error fetching Gist from GitHub API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("non-200 status code received from GitHub API: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body from GitHub API: %w", err)
	}

	// Parse the response to get the file content
	var gistResponse struct {
		Files map[string]struct {
			Filename string `json:"filename"`
			Content  string `json:"content"`
		} `json:"files"`
	}
	if err := json.Unmarshal(body, &gistResponse); err != nil {
		return nil, fmt.Errorf("error unmarshaling response from GitHub API: %w", err)
	}

	// Assuming the file name is known and constant
	gpuData, ok := gistResponse.Files["akashGpuDatabase.json"]
	if !ok {
		return nil, fmt.Errorf("GPU data file not found in Gist")
	}

	var gpuInfos []GPUInfo
	if err := json.Unmarshal([]byte(gpuData.Content), &gpuInfos); err != nil {
		return nil, fmt.Errorf("error unmarshaling GPU info: %w", err)
	}

	// Context for matchGPUsByDeviceID
	ctxGPU, cancelGPU := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelGPU()

	if err := matchGPUsByDeviceID(ctxGPU, node, gpuInfos); err != nil {
		return nil, err
	}

	// Context for nodeAllocatableDiscovery with a suitable timeout
	ctxAlloc, cancelAlloc := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelAlloc()

	if err := nodeAllocatableDiscovery(ctxAlloc, node); err != nil {
		return nil, err
	}

	return node, nil
}

func matchGPUsByDeviceID(ctx context.Context, node *v1.Node, gpuInfos []GPUInfo) error {
	files, err := os.ReadDir(pciDevicesDir)
	if err != nil {
		return fmt.Errorf("error reading PCI devices directory: %w", err)
	}

	for _, entry := range files {
		if err := ctx.Err(); err != nil {
			return err // Exiting early if context is cancelled
		}

		fileInfo, err := entry.Info()
		if err != nil {
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
				gpu := &v1.GPU{
					Info: v1.GPUInfo{
						Vendor:     gpuInfo.Vendor,
						Name:       gpuInfo.Name,
						ModelID:    gpuInfo.ModelID,
						Interface:  gpuInfo.Interface,
						MemorySize: gpuInfo.Memory,
					},
					Quantity: v1.ResourcePair{
						// Leave blank as these are set in the nodeAllocatableDiscovery function
					},
				}
				node.Gpus = append(node.Gpus, *gpu)
				break
			}
		}

		if len(node.Gpus) == 0 {
			log.Println("####GPU with following model ID must be added to GPU database: ", deviceID)
		}
	}

	return nil
}

func containsVendorID(content, vendorID string) bool {
	trimmedContent := strings.TrimSpace(content)
	return strings.HasSuffix(trimmedContent, vendorID)
}

func parseCPUInfo(ctx context.Context) (*v1.CPU, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	file, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return nil, fmt.Errorf("failed to open /proc/cpuinfo: %w", err)
	}
	defer file.Close()

	cpuInfoMap := make(map[string]*v1.CPUInfo)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err // return early if context is timed out
		}

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
		case "vendor_id", "model name", "cpu cores":
			if currentCPU, exists := cpuInfoMap[value]; exists {
				switch key {
				case "vendor_id":
					currentCPU.Vendor = value
				case "model name":
					currentCPU.Model = value
				case "cpu cores":
					cores, err := strconv.ParseUint(value, 10, 32)
					if err != nil {
						return nil, fmt.Errorf("failed to parse CPU cores from /proc/cpuinfo: %w", err)
					}
					currentCPU.Vcores = uint32(cores)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading from /proc/cpuinfo: %w", err)
	}

	// Dereference pointers when appending to cpuInfos
	cpuInfos := make([]v1.CPUInfo, 0, len(cpuInfoMap))
	for _, info := range cpuInfoMap {
		if info != nil {
			cpuInfos = append(cpuInfos, *info)
		}
	}

	cpu := &v1.CPU{
		Quantity: v1.ResourcePair{}, // Leave blank as this will be set later
		Info:     cpuInfos,          // Non-pointer slice
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

		// // Check if data collected has updated and only send to gRPC stream if there is an update
		// if !reflect.DeepEqual(currentNodeData, node2) {
		// 	currentNodeData = node2 // Update the current data with the new data

		// 	// Send stream to subscribed gRPC clients
		// 	fmt.Printf("Data sent to gRPC server stream: %+v\n", node2)
		// 	if err := stream.Send(&node2); err != nil {
		// 		return err
		// 	}
		// } else {
		// 	fmt.Println("No changes detected in node data.")
		// }

		if err := stream.Send(node); err != nil {
			return err
		}

		// Marshal the node to JSON for log entry
		nodeJSON, err := json.Marshal(node)
		if err != nil {
			log.Printf("Error marshaling node to JSON: %v", err)
			return err
		}

		log.Printf("Node data sent to gRPC server stream: %s", nodeJSON)

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

func nodeAllocatableDiscovery(ctx context.Context, node *v1.Node) error {
	// Get the node name from the environment variable set by the Downward API
	currentNodeName := os.Getenv("NODE_NAME")
	if currentNodeName == "" {
		return fmt.Errorf("NODE_NAME environment variable not set")
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error obtaining in-cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error creating Kubernetes client: %w", err)
	}

	// Fetch only the details for the current node using context
	knode, err := clientset.CoreV1().Nodes().Get(ctx, currentNodeName, metav1.GetOptions{})
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

	// Convert allocated resources to ResourcePair
	AllocatedGPUsQuantity := resource.NewQuantity(int64(AllocatedGPUs), resource.DecimalSI)
	allocatedCPUsQuantity := resource.NewQuantity(int64(allocatedCPUs), resource.DecimalSI)
	allocatedMemoryQuantity := resource.NewQuantity(int64(allocatedMemory), resource.DecimalSI)
	allocatedStorageQuantity := resource.NewQuantity(int64(allocatedStorage), resource.DecimalSI)

	// Update GPU quantities
	for i := range node.Gpus {
		node.Gpus[i].Quantity = v1.ResourcePair{
			Allocatable: gpuTotal.DeepCopy(),
			Allocated:   AllocatedGPUsQuantity.DeepCopy(),
		}
	}

	// Update CPU quantity
	node.CPU.Quantity = v1.ResourcePair{
		Allocatable: cpuTotal.DeepCopy(),
		Allocated:   allocatedCPUsQuantity.DeepCopy(),
	}

	// Update Memory and Storage info
	node.Memory.Quantity = v1.ResourcePair{
		Allocatable: memTotal.DeepCopy(),
		Allocated:   allocatedMemoryQuantity.DeepCopy(),
	}

	// Update Storage info
	if len(node.Storage) > 0 {
		for i := range node.Storage {
			node.Storage[i].Quantity = v1.ResourcePair{
				Allocatable: storageTotal.DeepCopy(),
				Allocated:   allocatedStorageQuantity.DeepCopy(),
			}
			// Update the StorageInfo field
			// Example: node.Storage[i].Info = ...
		}

	}

	return nil
}

func PrintResourcePair(rp *v1.ResourcePair) {
	if rp == nil {
		fmt.Println("ResourcePair is nil")
		return
	}

	allocatable := rp.GetAllocatable() // Get the allocatable Quantity
	allocated := rp.GetAllocated()     // Get the allocated Quantity

	fmt.Printf("ResourcePair:\nAllocatable: %s\nAllocated: %s\n", allocatable.String(), allocated.String())

	// If you want to print attributes as well
	if attributes := rp.GetAttributes(); len(attributes) > 0 {
		fmt.Println("Attributes:")
		for _, attr := range attributes {
			// Assuming each attribute has a String() or similar method for printing
			fmt.Printf(" - %v\n", attr)
		}
	}
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
