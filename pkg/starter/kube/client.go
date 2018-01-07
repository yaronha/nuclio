package kube

import (
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/platform/kube/functioncr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/pkg/errors"
	"github.com/nuclio/nuclio/pkg/starter/core"
)

const NUCLIO_SELECTOR = "serverless=nuclio"

func NewKubeClient(logger nuclio.Logger, kubeconf, namespace string, reqChannel chan *core.AsyncRequests) (*kubeClient, error) {
	newClient := &kubeClient{logger:logger, namespace:namespace, kubeconf:kubeconf}
	newClient.reqChannel = reqChannel

	fi, err := NewFunctionInterface(newClient)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	newClient.Function = fi

	err = newClient.newClientConf()

	return newClient, err
}

type kubeClient struct {
	Function     *functionIfc

	logger       nuclio.Logger
	kubeClient   *kubernetes.Clientset
	restConfig   *rest.Config
	functionCR   *functioncr.Client
	namespace    string
	kubeconf     string
	reqChannel   chan *core.AsyncRequests
}

func (kc *kubeClient) newClientConf() error {

	var err error
	kc.restConfig, err = clientcmd.BuildConfigFromFlags("", kc.kubeconf)
	if err != nil {
		return errors.Wrap(err, "Failed to create REST config")
	}

	kc.kubeClient, err = kubernetes.NewForConfig(kc.restConfig)
	if err != nil {
		return err
	}

	// create a client for function custom resources
	kc.functionCR, err = functioncr.NewClient(kc.logger, kc.restConfig, kc.kubeClient)
	if err != nil {
		panic(errors.Wrap(err, "Failed to create function custom resource client"))
	}

	return nil
}

func (kc *kubeClient) Start() error {

	return nil
}

func (kc *kubeClient) ListEPs() error {

	opts := meta_v1.ListOptions{
		LabelSelector: NUCLIO_SELECTOR,
	}

	result, err := kc.kubeClient.CoreV1().Endpoints(kc.namespace).List(opts)
	if err != nil {
		return errors.Wrap(err, "Failed to list deployments")
	}

	for _, ep := range result.Items {
		epItem := core.FunctionEndPoints{Namespace:ep.Namespace, Name:ep.Name, Version:ep.Labels["version"]}
		ips := []string{}
		port := 8080
		epItem.APIPort = port
		for _, subset := range ep.Subsets {
			for _, addr := range subset.Addresses {
				ips = append(ips, addr.IP)
			}
		}
		epItem.IPs = ips
		kc.logger.InfoWith("got EPs", "ep", epItem)

		kc.reqChannel <- &core.AsyncRequests{ Type: core.RequestTypeUpdateEndPoints, Data: &epItem}

	}

	return nil
}