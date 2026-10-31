# Cilium v1.18.12 admission fixture

`ciliumnetworkpolicies.yaml` is the unmodified complete CiliumNetworkPolicy v2
CustomResourceDefinition from the official Cilium v1.18.12 release source:

https://github.com/cilium/cilium/blob/v1.18.12/pkg/k8s/apis/cilium.io/client/crds/v2/ciliumnetworkpolicies.yaml

The upstream Apache-2.0 license is retained in `LICENSE` from:

https://github.com/cilium/cilium/blob/v1.18.12/LICENSE

The disposable K3s platform admission test creates this CRD and waits for
`Established` before validating the full rendered resources and Pod specs with
server dry-run. The fixture is available offline; tests do not download it,
install Cilium workloads, or alter the K3s CNI. This exercises Kubernetes CRD
schema admission, not Cilium dataplane policy enforcement.
