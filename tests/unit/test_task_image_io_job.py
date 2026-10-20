"""Exercise frozen scheduler input, including Kubernetes field-selector shape."""
from copy import deepcopy

from loom_control_plane.task_image_capacity import _bind_io_job


def _native():
    return {"job": {"metadata": {}, "spec": {"activeDeadlineSeconds": 1800, "template": {
        "spec": {"nodeSelector": {"pool": "execution"}, "automountServiceAccountToken": False,
                 "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
                     "nodeSelectorTerms": [
                         {"matchExpressions": [{"key": "zone", "operator": "In", "values": ["a"]}]},
                         {"matchExpressions": [{"key": "zone", "operator": "In", "values": ["b"]}]},
                     ],
                 }}}},
    }}}}


def test_node_binding_intersects_every_required_term_without_rewriting_target():
    original = _native()
    before = deepcopy(original)
    binding = {"mode": "node", "node_name": "healthy", "node_uid": "uid-1"}
    result = _bind_io_job(original, binding)
    assert original == before
    pod = result["job"]["spec"]["template"]["spec"]
    assert "nodeName" not in pod and not pod["automountServiceAccountToken"]
    assert pod["nodeSelector"] == {"pool": "execution"}
    terms = pod["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]
    assert all(term["matchFields"] == [{"key": "metadata.name", "operator": "In", "values": ["healthy"]}]
               for term in terms)
    assert [term["matchExpressions"] for term in terms] == [
        term["matchExpressions"] for term in before["job"]["spec"]["template"]["spec"]["affinity"]["nodeAffinity"][
            "requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]]
    assert result["job"]["metadata"]["annotations"]["loom.openai.com/build-node-uid"] == "uid-1"
    assert result["io_binding"] == binding
    assert result["job"]["spec"]["activeDeadlineSeconds"] == 1800


def test_bootstrap_excludes_all_observed_nodes_with_single_value_field_requirements():
    result = _bind_io_job(_native(), {"mode": "bootstrap", "excluded_node_names": ["hot-a", "hot-b"]})
    terms = result["job"]["spec"]["template"]["spec"]["affinity"]["nodeAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]
    assert all(term["matchFields"] == [
        {"key": "metadata.name", "operator": "NotIn", "values": ["hot-a"]},
        {"key": "metadata.name", "operator": "NotIn", "values": ["hot-b"]},
    ] for term in terms)
    assert "annotations" not in result["job"]["metadata"]


def test_scale_zero_bootstrap_keeps_existing_autoscaler_compatible_selectors():
    original = _native()
    result = _bind_io_job(original, {"mode": "bootstrap", "excluded_node_names": []})
    assert result["job"] == original["job"]
