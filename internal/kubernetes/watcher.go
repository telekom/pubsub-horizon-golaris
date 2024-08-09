// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

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
			DeleteFunc: func(obj any) {
				handlePodEvent(obj)
			},
		})

	stopChannel := make(chan struct{})
	go func() {
		log.Info().Msgf("Starting kubernetes pod watcher")
		controller.Run(stopChannel)
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
	if pod, ok := obj.(*v1.Pod); ok {
		if strings.Contains(pod.Name, config.Current.OldGolarisName) {
			log.Info().Msgf("Pod %s has been deleted", pod.Name)
			handler.CheckWaitingEvents()
		}
	}
}
