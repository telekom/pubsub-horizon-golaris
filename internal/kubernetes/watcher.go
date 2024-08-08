package kubernetes

import (
	"flag"
	"github.com/rs/zerolog/log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	kubeCache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/handler"
	"strings"
	"time"
)

var kubeconfig string

func Initialize() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to the kubeconfig file")
	flag.Parse()

	kubernetesPodWatcher()
	log.Info().Msgf("Kubernetes watcher started")
}

func kubernetesPodWatcher() {
	kubeConfig, err := buildConfig(kubeconfig)
	if err != nil {
		log.Error().Msgf("Error while building kubeconfig: %v", err)
		return
	}

	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		log.Error().Msgf("Error while creating clientset: %v", err)
		return
	}

	podWatcher := kubeCache.NewListWatchFromClient(
		clientset.CoreV1().RESTClient(),
		"pods",
		config.Current.Kubernetes.Namespace,
		fields.Everything(),
	)

	_, controller := kubeCache.NewInformer(
		podWatcher,
		&v1.Pod{},
		time.Second*30,
		kubeCache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				handlePodEvent(obj)
			},
			DeleteFunc: func(obj any) {
				handlePodEvent(obj)
			},
		})

	stopChannel := make(chan struct{})
	go func() {
		log.Info().Msgf("Starting pod watcher")
		controller.Run(stopChannel)
		log.Info().Msgf("Pod watcher stopped")
	}()
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
		return cfg, nil
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func handlePodEvent(obj any) {
	// Check if the event is a pod event
	if pod, ok := obj.(*v1.Pod); ok {
		// Check if the pod is a Quasar pod
		if strings.Contains(pod.Name, "horizon-quasar") {
			log.Info().Msgf("Pod name is: %s", pod.Name)
			// Check if the pod is restarted
			if pod.Status.Phase == v1.PodRunning {
				handler.CheckWaitingEvents()
			}
		}
	}
}
