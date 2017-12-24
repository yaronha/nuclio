package kubewatch

import (
	"encoding/json"
	"fmt"
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"github.com/pkg/errors"
	"k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"time"
)

const NUCLIO_SELECTOR = "serverless=test"

func GetClientConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

type Watcher struct {
	managerContext *jobs.ManagerContext
	logger         nuclio.Logger
	namespace      string
}

func (w *Watcher) dispatchChange(message *jobs.RequestMessage) {
	/*
		w.logger.DebugWith("Dispatching change",
			"kind", message.Type,
			"name", message.Name,
			"function", message.Function,
			"obj", message.Object)
	*/

	w.managerContext.RequestsChannel <- message
}

func NewPodWatcher(client *kubernetes.Clientset, managerContext *jobs.ManagerContext, logger nuclio.Logger, namespace string) error {
	newWatcher := &Watcher{
		logger:         logger.GetChild("podWatcher").(nuclio.Logger),
		namespace:      namespace,
		managerContext: managerContext,
	}

	newWatcher.logger.Debug("Watching for POD changes")

	opts := meta_v1.ListOptions{
		LabelSelector: NUCLIO_SELECTOR,
	}

	listWatch := &cache.ListWatch{
		ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
			return client.Pods(namespace).List(opts)
		},
		WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
			return client.Pods(namespace).Watch(opts)
		},
	}

	_, controller := cache.NewInformer(
		listWatch,
		&v1.Pod{},
		time.Minute*10,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				proc := getProcStruct(obj.(*v1.Pod))
				proc.State = getPodState(obj.(*v1.Pod))
				newWatcher.dispatchChange(&jobs.RequestMessage{
					Type: jobs.RequestTypeProcUpdateState, Object: proc})
			},
			DeleteFunc: func(obj interface{}) {
				proc := getProcStruct(obj.(*v1.Pod))
				proc.State = jobs.ProcessStateDelete
				newWatcher.dispatchChange(&jobs.RequestMessage{
					Name: proc.Name, Namespace: proc.Namespace,
					Type: jobs.RequestTypeProcDel, Object: proc})
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldPod := oldObj.(*v1.Pod)
				newPod := newObj.(*v1.Pod)
				if oldPod.ResourceVersion != newPod.ResourceVersion {
					proc := getProcStruct(newPod)
					proc.State = getPodState(newPod)
					newWatcher.dispatchChange(&jobs.RequestMessage{
						Type: jobs.RequestTypeProcUpdateState, Object: proc})
				} else {
					newWatcher.dispatchChange(&jobs.RequestMessage{
						Type: jobs.RequestTypeProcHealth, Name: newPod.Name, Namespace: newPod.Namespace})
				}
			},
		},
	)

	// run the watcher. TODO: pass a channel that can receive stop requests, when stop is supported
	go controller.Run(make(chan struct{}))

	return nil
}

func getProcStruct(pod *v1.Pod) *jobs.BaseProcess {
	proc := jobs.BaseProcess{
		Name: pod.Name, Namespace: pod.Namespace,
		Function: pod.Labels["name"],
		Version:  pod.Labels["version"],
		Alias:    pod.Labels["alias"],
		IP:       pod.Status.PodIP,
	}
	return &proc
}

func getPodState(pod *v1.Pod) jobs.ProcessState {
	status := pod.Status

	switch status.Phase {
	case "Unknown":
		return jobs.ProcessStateUnknown
	case "Failed":
		return jobs.ProcessStateFailed
	case "Running":
		if pod.Status.PodIP != "" {
			for _, cond := range status.Conditions {
				if cond.Type == "Ready" {
					return jobs.ProcessStateReady
				}
			}
		}
		return jobs.ProcessStateNotReady
	default:
		return jobs.ProcessStateNotReady
	}

}

func isPodNewer(a *v1.Pod, b *v1.Pod) bool {
	t1 := a.ObjectMeta.CreationTimestamp
	t2 := b.ObjectMeta.CreationTimestamp
	return t2.Before(t1)
}

func NewDeployWatcher(client *kubernetes.Clientset, managerContext *jobs.ManagerContext, logger nuclio.Logger, namespace string) error {
	newWatcher := &Watcher{
		logger:         logger.GetChild("deployWatcher").(nuclio.Logger),
		namespace:      namespace,
		managerContext: managerContext,
	}

	newWatcher.logger.Debug("Watching for Deployment changes")

	opts := meta_v1.ListOptions{
		LabelSelector: NUCLIO_SELECTOR,
	}

	listWatch := &cache.ListWatch{
		ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
			return client.AppsV1beta1Client.Deployments(namespace).List(opts)
		},
		WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
			return client.AppsV1beta1Client.Deployments(namespace).Watch(opts)
		},
	}

	_, controller := cache.NewInformer(
		listWatch,
		&v1beta1.Deployment{},
		time.Minute*10,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				dep := getDeployStruct(obj.(*v1beta1.Deployment))
				newWatcher.dispatchChange(&jobs.RequestMessage{
					Type: jobs.RequestTypeDeployUpdate, Object: dep})
			},
			DeleteFunc: func(obj interface{}) {
				dep := getDeployStruct(obj.(*v1beta1.Deployment))
				newWatcher.dispatchChange(&jobs.RequestMessage{
					Type: jobs.RequestTypeDeployRemove, Object: dep})
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldDep := oldObj.(*v1beta1.Deployment)
				newDep := newObj.(*v1beta1.Deployment)
				if oldDep.ObjectMeta.Generation != newDep.ObjectMeta.Generation {
					newWatcher.dispatchChange(&jobs.RequestMessage{
						Type: jobs.RequestTypeDeployUpdate, Object: getDeployStruct(newDep)})
				}
			},
		},
	)

	// run the watcher. TODO: pass a channel that can receive stop requests, when stop is supported
	go controller.Run(make(chan struct{}))

	return nil
}

func getDeployStruct(deploy *v1beta1.Deployment) *jobs.DeploymentSpec {
	depBase := jobs.BaseDeployment{
		Name: deploy.Name, Namespace: deploy.Namespace,
		Function:     deploy.Labels["name"],
		Version:      deploy.Labels["version"],
		Alias:        deploy.Labels["alias"],
		ExpectedProc: int(*deploy.Spec.Replicas),
	}
	depBase.FuncGen = deploy.Annotations["func_gen"]
	dep := jobs.DeploymentSpec{BaseDeployment: depBase}

	funcJson, ok := deploy.Annotations["func_json"]
	if ok {
		fn := funcStruct{}
		err := json.Unmarshal([]byte(funcJson), &fn)
		if err == nil {
			dep.Triggers = []*jobs.BaseJob{}
			for name, trigger := range fn.Triggers {
				dep.Triggers = append(dep.Triggers, &jobs.BaseJob{Name: name, TotalTasks: trigger.Partitions, MaxTaskAllocation: trigger.MaxTasks})
			}
		} else {
			fmt.Println("err", funcJson)
		}
		//fmt.Println(fn)

	}
	//fmt.Printf("Status %+v\n", deploy.Status)
	return &dep
}

type funcStruct struct {
	Triggers map[string]trigStruct `json:"triggers"`
}

type trigStruct struct {
	Class      string `json:"class"`
	Kind       string `json:"kind"`
	Partitions int    `json:"partitions"`
	MaxTasks   int    `json:"maxTasks"`
}

func ListDeployments(client *kubernetes.Clientset, logger nuclio.Logger, namespace string) ([]*jobs.DeploymentSpec, error) {

	listOptions := meta_v1.ListOptions{
		LabelSelector: NUCLIO_SELECTOR,
	}

	result, err := client.AppsV1beta1().Deployments(namespace).List(listOptions)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to list deployments")
	}

	logger.DebugWith("Got deployments", "num", len(result.Items))

	depList := []*jobs.DeploymentSpec{}
	for _, deployment := range result.Items {
		depList = append(depList, getDeployStruct(&deployment))
	}

	return depList, nil
}

func ListPods(client *kubernetes.Clientset, logger nuclio.Logger, namespace string) ([]*jobs.BaseProcess, error) {

	listOptions := meta_v1.ListOptions{
		LabelSelector: NUCLIO_SELECTOR,
	}

	result, err := client.Core().Pods(namespace).List(listOptions)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to list pods")
	}

	logger.DebugWith("Got pods", "num", len(result.Items))

	procList := []*jobs.BaseProcess{}
	for _, pod := range result.Items {
		proc := getProcStruct(&pod)
		proc.State = getPodState(&pod)
		procList = append(procList, proc)
	}

	return procList, nil
}
