package kubernetes

import (
	"context"
	"flag"
	"fmt"
	"github.com/rs/zerolog/log"
	"golaris/config"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"path/filepath"
)

func InitializeKubernetesClient() *kubernetes.Clientset {
	var kubeConfig *string

	// Determine the home directory to construct the kubeConfig file path
	if home := homeDir(); home != "" {
		kubeConfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "absolute path to the kubeconfig file")
	} else {
		kubeConfig = flag.String("kubeconfig", "", "absolute path to the kubeConfig file")
	}

	// Parse command-line flags to retrieve kubeConfig file path
	flag.Parse()

	// Build Kubernetes client configuration from the specified kubeConfig file
	kubernetesConfig, err := clientcmd.BuildConfigFromFlags("", *kubeConfig)
	if err != nil {
		panic(err.Error())
	}

	// Create a new Kubernetes client based on the configuration
	clientSet, err := kubernetes.NewForConfig(kubernetesConfig)
	if err != nil {
		panic(err.Error())
	}
	return clientSet
}

func PodsHealthAfterRestart(clientSet *kubernetes.Clientset) bool {
	watcher, err := clientSet.CoreV1().Pods(config.Current.Kubernetes.Namespace).Watch(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Error().Err(err).Msgf("Error while watching pods: %v", err)
		return false
	}

	// Listen for pod events and react on modifications
	for {
		select {
		// Wait until an event is received by the watcher
		case event := <-watcher.ResultChan():
			// Pod restart triggers a modified event
			if event.Type == watch.Modified {
				// Make sure that the received event is actually a pod
				pod := event.Object.(*v1.Pod)
				if pod.Status.Phase == "Running" {
					fmt.Println("Pod is running, checking all pods...")
					if allPodsAreRunning(clientSet) {
						return true
					}
				}
			}
		}
	}
}

func allPodsAreRunning(clientSet *kubernetes.Clientset) bool {
	pods, err := clientSet.CoreV1().Pods(config.Current.Kubernetes.Namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Error().Err(err).Msgf("Error while getting pods: %v", err)
		return false
	}

	// Iterate through pods and check if any pod is not running
	for _, pod := range pods.Items {
		if pod.Status.Phase != "Running" {
			return false
		}
	}
	return true
}

func homeDir() string {
	if home := os.Getenv("HOME"); home != "" {
		return home
	}
	return os.Getenv("USERPROFILE") // windows
}
