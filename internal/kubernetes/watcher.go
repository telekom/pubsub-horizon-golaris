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
}

func kubernetesPodWatcher() {
	log.Info().Msg("Building kubeconfig...")
	kubeConfig, err := buildConfig(kubeconfig)
	if err != nil {
		log.Error().Msgf("Error while building kubeconfig: %v", err)
		return
	}
	log.Info().Msg("Kubeconfig built successfully.")

	log.Info().Msg("Creating Kubernetes clientset...")
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		log.Error().Msgf("Error while creating clientset: %v", err)
		return
	}
	log.Info().Msg("Clientset created successfully.")

	podWatcher := kubeCache.NewListWatchFromClient(
		clientset.CoreV1().RESTClient(),
		"pods",
		config.Current.Kubernetes.Namespace,
		fields.Everything(),
	)
	log.Info().Msgf("Watching pods in namespace: %v", config.Current.Kubernetes.Namespace)

	_, controller := kubeCache.NewInformer(
		podWatcher,
		&v1.Pod{},
		time.Second*30,
		kubeCache.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj, newObj any) {
				handlePodEvent(newObj)
				log.Info().Msgf("Pod updated: %v", newObj)
			},
		})

	stopChannel := make(chan struct{})
	go func() {
		log.Info().Msg("Starting controller...")
		controller.Run(stopChannel)
		log.Info().Msg("Controller stopped.")
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
	log.Info().Msgf("Handling pod event: %v", obj)
	// Check if the event is a pod event
	if pod, ok := obj.(*v1.Pod); ok {
		// Check if the pod is a Quasar pod
		log.Info().Msgf("Pod name: %v", pod.Name)
		if strings.Contains(pod.Name, "horizon-quasar") {
			// Check if the pod is restarted
			if pod.Status.Phase == v1.PodRunning {
				handler.CheckWaitingEvents()
			}
		}
	}
}
