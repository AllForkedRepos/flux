package kubernetes

import (
	apiapps "k8s.io/api/apps/v1"
	apibatch "k8s.io/api/batch/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/weaveworks/flux"
	fhr_v1alpha2 "github.com/weaveworks/flux/apis/helm.integrations.flux.weave.works/v1alpha2"
	"github.com/weaveworks/flux/cluster"
	kresource "github.com/weaveworks/flux/cluster/kubernetes/resource"
	"github.com/weaveworks/flux/image"
	"github.com/weaveworks/flux/resource"
)

// AntecedentAnnotation is an annotation on a resource indicating that
// the cause of that resource (indirectly, via a Helm release) is a
// FluxHelmRelease. We use this rather than the `OwnerReference` type
// built into Kubernetes so that there are no garbage-collection
// implications. The value is expected to be a serialised
// `flux.ResourceID`.
const AntecedentAnnotation = "flux.weave.works/antecedent"

/////////////////////////////////////////////////////////////////////////////
// Kind registry

type resourceKind interface {
	getPodController(c *Cluster, namespace, name string) (podController, error)
	getPodControllers(c *Cluster, namespace string) ([]podController, error)
}

var (
	resourceKinds = make(map[string]resourceKind)
)

func init() {
	resourceKinds["cronjob"] = &cronJobKind{}
	resourceKinds["daemonset"] = &daemonSetKind{}
	resourceKinds["deployment"] = &deploymentKind{}
	resourceKinds["statefulset"] = &statefulSetKind{}
	resourceKinds["fluxhelmrelease"] = &fluxHelmReleaseKind{}
}

type podController struct {
	k8sObject
	apiVersion  string
	kind        string
	name        string
	status      string
	pods        cluster.Pods
	errors      []string
	podTemplate apiv1.PodTemplateSpec
}

func (pc podController) toClusterController(resourceID flux.ResourceID) cluster.Controller {
	var clusterContainers []resource.Container
	var excuse string
	for _, container := range pc.podTemplate.Spec.Containers {
		ref, err := image.ParseRef(container.Image)
		if err != nil {
			clusterContainers = nil
			excuse = err.Error()
			break
		}
		clusterContainers = append(clusterContainers, resource.Container{Name: container.Name, Image: ref})
	}
	for _, container := range pc.podTemplate.Spec.InitContainers {
		ref, err := image.ParseRef(container.Image)
		if err != nil {
			clusterContainers = nil
			excuse = err.Error()
			break
		}
		clusterContainers = append(clusterContainers, resource.Container{Name: container.Name, Image: ref})
	}

	var antecedent flux.ResourceID
	if ante, ok := pc.GetAnnotations()[AntecedentAnnotation]; ok {
		id, err := flux.ParseResourceID(ante)
		if err == nil {
			antecedent = id
		}
	}

	return cluster.Controller{
		ID:         resourceID,
		Status:     pc.status,
		Pods:       pc.pods,
		Errors:     pc.errors,
		Antecedent: antecedent,
		Labels:     pc.GetLabels(),
		Containers: cluster.ContainersOrExcuse{Containers: clusterContainers, Excuse: excuse},
	}
}

/////////////////////////////////////////////////////////////////////////////
// extensions/v1beta1 Deployment

type deploymentKind struct{}

func (dk *deploymentKind) getPodController(c *Cluster, namespace, name string) (podController, error) {
	deployment, err := c.client.AppsV1().Deployments(namespace).Get(name, meta_v1.GetOptions{})
	if err != nil {
		return podController{}, err
	}

	return makeDeploymentPodController(deployment), nil
}

func (dk *deploymentKind) getPodControllers(c *Cluster, namespace string) ([]podController, error) {
	deployments, err := c.client.AppsV1().Deployments(namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var podControllers []podController
	for i := range deployments.Items {
		podControllers = append(podControllers, makeDeploymentPodController(&deployments.Items[i]))
	}

	return podControllers, nil
}

func deploymentErrors(d *apiapps.Deployment) []string {
	var errs []string
	for _, cond := range d.Status.Conditions {
		if (cond.Type == apiapps.DeploymentAvailable && cond.Status == apiv1.ConditionFalse) ||
			(cond.Type == apiapps.DeploymentProgressing && cond.Status == apiv1.ConditionFalse) ||
			(cond.Type == apiapps.DeploymentReplicaFailure && cond.Status == apiv1.ConditionTrue) {
			errs = append(errs, cond.Message)
		}
	}
	return errs
}

func makeDeploymentPodController(deployment *apiapps.Deployment) podController {
	var status string
	objectMeta, deploymentStatus := deployment.ObjectMeta, deployment.Status

	status = StatusStarted
	errs := deploymentErrors(deployment)
	pods := cluster.Pods{
		Desired:   *deployment.Spec.Replicas,
		Updated:   deploymentStatus.UpdatedReplicas,
		Ready:     deploymentStatus.ReadyReplicas,
		Available: deploymentStatus.AvailableReplicas,
		Outdated:  deploymentStatus.Replicas - deploymentStatus.UpdatedReplicas,
	}

	if deploymentStatus.ObservedGeneration >= objectMeta.Generation {
		// the definition has been updated; now let's see about the replicas
		status = StatusUpdating
		if pods.Ready == pods.Desired && pods.Available == pods.Desired && pods.Outdated == 0 {
			status = StatusReady
		}
		if len(errs) != 0 {
			status = StatusError
		}
	}

	return podController{
		apiVersion:  "apps/v1",
		kind:        "Deployment",
		name:        deployment.ObjectMeta.Name,
		status:      status,
		pods:        pods,
		errors:      errs,
		podTemplate: deployment.Spec.Template,
		k8sObject:   deployment}
}

/////////////////////////////////////////////////////////////////////////////
// extensions/v1beta daemonset

type daemonSetKind struct{}

func (dk *daemonSetKind) getPodController(c *Cluster, namespace, name string) (podController, error) {
	daemonSet, err := c.client.AppsV1().DaemonSets(namespace).Get(name, meta_v1.GetOptions{})
	if err != nil {
		return podController{}, err
	}

	return makeDaemonSetPodController(daemonSet), nil
}

func (dk *daemonSetKind) getPodControllers(c *Cluster, namespace string) ([]podController, error) {
	daemonSets, err := c.client.AppsV1().DaemonSets(namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var podControllers []podController
	for i := range daemonSets.Items {
		podControllers = append(podControllers, makeDaemonSetPodController(&daemonSets.Items[i]))
	}

	return podControllers, nil
}

func makeDaemonSetPodController(daemonSet *apiapps.DaemonSet) podController {
	var status string
	objectMeta, daemonSetStatus := daemonSet.ObjectMeta, daemonSet.Status

	status = StatusUpdating
	pods := cluster.Pods{
		Desired:   daemonSetStatus.DesiredNumberScheduled,
		Updated:   daemonSetStatus.UpdatedNumberScheduled,
		Ready:     daemonSetStatus.NumberReady,
		Available: daemonSetStatus.NumberAvailable,
		Outdated:  daemonSetStatus.CurrentNumberScheduled - daemonSetStatus.UpdatedNumberScheduled,
	}

	if daemonSetStatus.ObservedGeneration >= objectMeta.Generation {
		// the definition has been updated; now let's see about the replicas
		status = StatusUpdating
		if pods.Ready == pods.Desired && pods.Available == pods.Desired && pods.Outdated == 0 {
			status = StatusReady
		}
	}

	return podController{
		apiVersion:  "apps/v1",
		kind:        "DaemonSet",
		name:        daemonSet.ObjectMeta.Name,
		status:      status,
		pods:        pods,
		podTemplate: daemonSet.Spec.Template,
		k8sObject:   daemonSet}
}

/////////////////////////////////////////////////////////////////////////////
// apps/v1beta1 StatefulSet

type statefulSetKind struct{}

func (dk *statefulSetKind) getPodController(c *Cluster, namespace, name string) (podController, error) {
	statefulSet, err := c.client.AppsV1().StatefulSets(namespace).Get(name, meta_v1.GetOptions{})
	if err != nil {
		return podController{}, err
	}

	return makeStatefulSetPodController(statefulSet), nil
}

func (dk *statefulSetKind) getPodControllers(c *Cluster, namespace string) ([]podController, error) {
	statefulSets, err := c.client.AppsV1().StatefulSets(namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var podControllers []podController
	for i := range statefulSets.Items {
		podControllers = append(podControllers, makeStatefulSetPodController(&statefulSets.Items[i]))
	}

	return podControllers, nil
}

func makeStatefulSetPodController(statefulSet *apiapps.StatefulSet) podController {
	var status string
	objectMeta, statefulSetStatus := statefulSet.ObjectMeta, statefulSet.Status

	status = StatusUpdating
	pods := cluster.Pods{
		Desired:  *statefulSet.Spec.Replicas,
		Updated:  statefulSetStatus.UpdatedReplicas,
		Ready:    statefulSetStatus.ReadyReplicas,
		Outdated: statefulSetStatus.CurrentReplicas - statefulSetStatus.UpdatedReplicas,
	}

	// The type of ObservedGeneration is *int64, unlike other controllers.
	if statefulSetStatus.ObservedGeneration >= objectMeta.Generation {
		// the definition has been updated; now let's see about the replicas
		status = StatusUpdating
		if pods.Ready == pods.Desired && pods.Updated == pods.Desired && pods.Outdated == 0 {
			status = StatusReady
		}
	}

	return podController{
		apiVersion:  "apps/v1",
		kind:        "StatefulSet",
		name:        statefulSet.ObjectMeta.Name,
		status:      status,
		pods:        pods,
		podTemplate: statefulSet.Spec.Template,
		k8sObject:   statefulSet}
}

/////////////////////////////////////////////////////////////////////////////
// batch/v1beta1 CronJob

type cronJobKind struct{}

func (dk *cronJobKind) getPodController(c *Cluster, namespace, name string) (podController, error) {
	cronJob, err := c.client.BatchV1beta1().CronJobs(namespace).Get(name, meta_v1.GetOptions{})
	if err != nil {
		return podController{}, err
	}

	return makeCronJobPodController(cronJob), nil
}

func (dk *cronJobKind) getPodControllers(c *Cluster, namespace string) ([]podController, error) {
	cronJobs, err := c.client.BatchV1beta1().CronJobs(namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var podControllers []podController
	for i, _ := range cronJobs.Items {
		podControllers = append(podControllers, makeCronJobPodController(&cronJobs.Items[i]))
	}

	return podControllers, nil
}

func makeCronJobPodController(cronJob *apibatch.CronJob) podController {
	return podController{
		apiVersion:  "batch/v1beta1",
		kind:        "CronJob",
		name:        cronJob.ObjectMeta.Name,
		status:      StatusReady,
		podTemplate: cronJob.Spec.JobTemplate.Spec.Template,
		k8sObject:   cronJob}
}

/////////////////////////////////////////////////////////////////////////////
// helm.integrations.flux.weave.works/v1alpha2 FluxHelmRelease

type fluxHelmReleaseKind struct{}

func (fhr *fluxHelmReleaseKind) getPodController(c *Cluster, namespace, name string) (podController, error) {
	fluxHelmRelease, err := c.client.HelmV1alpha2().FluxHelmReleases(namespace).Get(name, meta_v1.GetOptions{})
	if err != nil {
		return podController{}, err
	}

	return makeFluxHelmReleasePodController(fluxHelmRelease), nil
}

func (fhr *fluxHelmReleaseKind) getPodControllers(c *Cluster, namespace string) ([]podController, error) {
	fluxHelmReleases, err := c.client.HelmV1alpha2().FluxHelmReleases(namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var podControllers []podController
	for _, f := range fluxHelmReleases.Items {
		podControllers = append(podControllers, makeFluxHelmReleasePodController(&f))
	}

	return podControllers, nil
}

func makeFluxHelmReleasePodController(fluxHelmRelease *fhr_v1alpha2.FluxHelmRelease) podController {
	containers := createK8sFHRContainers(fluxHelmRelease.Spec)

	podTemplate := apiv1.PodTemplateSpec{
		ObjectMeta: fluxHelmRelease.ObjectMeta,
		Spec: apiv1.PodSpec{
			Containers:       containers,
			ImagePullSecrets: []apiv1.LocalObjectReference{},
		},
	}

	return podController{
		apiVersion:  "helm.integrations.flux.weave.works/v1alpha2",
		kind:        "FluxHelmRelease",
		name:        fluxHelmRelease.ObjectMeta.Name,
		status:      fluxHelmRelease.Status.ReleaseStatus,
		podTemplate: podTemplate,
		k8sObject:   fluxHelmRelease,
	}
}

// createK8sContainers creates a list of k8s containers by
// interpreting the FluxHelmRelease resource. The interpretation is
// analogous to that in cluster/kubernetes/resource/fluxhelmrelease.go
func createK8sFHRContainers(spec fhr_v1alpha2.FluxHelmReleaseSpec) []apiv1.Container {
	var containers []apiv1.Container
	_ = kresource.FindFluxHelmReleaseContainers(spec.Values, func(name string, image image.Ref, _ kresource.ImageSetter) error {
		containers = append(containers, apiv1.Container{
			Name:  name,
			Image: image.String(),
		})
		return nil
	})
	return containers
}
