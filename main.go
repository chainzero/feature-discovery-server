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

	// Create a new HTTP request with context
	req, err := http.NewRequestWithContext(ctxHTTP, "GET", jsonURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	// Use http.Client to do the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error fetching GPU data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
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

	var currentCPU *v1.CPUInfo
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			fmt.Println("within context error")
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

		jsonNode, err := json.Marshal(node)
		if err != nil {
			log.Fatalf("Error occurred during marshalling: %s", err)
		}

		fmt.Println("jsonNode in QueryNode: ", string(jsonNode))

		fmt.Println("###Node in QueryNode: ", node)

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

	jsonGpus, err := json.Marshal(gpus)
	if err != nil {
		log.Fatalf("Error occurred during marshalling: %s", err)
	}

	fmt.Println("jsonGpus: ", string(jsonGpus))

	fmt.Println("###gpus: ", gpus)

	fmt.Println("Output from PrintResourcePair:")
	PrintResourcePair(gpus.Quantity)

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

	jsonNode, err := json.Marshal(node)
	if err != nil {
		log.Fatalf("Error occurred during marshalling: %s", err)
	}

	fmt.Println("jsonNode at end of nodeAllocatableDiscovery: ", string(jsonNode))

	fmt.Println("###Node at end of nodeAllocatableDiscovery: ", node)

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
