package kube

import (
	"github.com/nuclio/nuclio/pkg/platform/kube/functioncr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/pkg/errors"
	"github.com/nuclio/nuclio/pkg/starter/core"
	"k8s.io/apimachinery/pkg/watch"
	"time"
	"k8s.io/client-go/tools/cache"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/api/core/v1"
	"github.com/nuclio/logger"
)

const NUCLIO_SELECTOR = "serverless=nuclio"

func NewKubeClient(logger logger.Logger, kubeconf, namespace string, reqChannel chan *core.AsyncRequests) (*kubeClient, error) {
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

	logger       logger.Logger
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


func (kc *kubeClient) processEP(ep *v1.Endpoints) {

	epItem := core.FunctionEndPoints{Namespace:ep.Namespace, Name:ep.Labels["name"], Version:ep.Labels["version"]}
	ips := []string{}
	port := 8080
	epItem.APIPort = port
	for _, subset := range ep.Subsets {
		for _, addr := range subset.Addresses {
			ips = append(ips, addr.IP)
		}
	}
	epItem.IPs = ips

	kc.reqChannel <- &core.AsyncRequests{ Type: core.RequestTypeUpdateEndPoints, Data: &epItem}

}

func (kc *kubeClient) NewEPWatcher() error {

	logger := kc.logger.GetChild("epWatcher").(logger.Logger)
	logger.Debug("Watching for Endpoint changes")

	opts := meta_v1.ListOptions{
		LabelSelector: NUCLIO_SELECTOR,
	}

	listWatch := &cache.ListWatch{
		ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
			return kc.kubeClient.Endpoints(kc.namespace).List(opts)
		},
		WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
			return kc.kubeClient.Endpoints(kc.namespace).Watch(opts)
		},
	}

	_, controller := cache.NewInformer(
		listWatch,
		&v1.Endpoints{},
		time.Minute*10,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				kc.processEP(obj.(*v1.Endpoints))
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				kc.processEP(newObj.(*v1.Endpoints))
			},
			DeleteFunc: func(obj interface{}) {

			},
		},
	)

	// run the watcher. TODO: pass a channel that can receive stop requests, when stop is supported
	go controller.Run(make(chan struct{}))

	return nil
}
