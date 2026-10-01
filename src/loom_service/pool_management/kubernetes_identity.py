"""Closed workload comparison with explicit, harmless Kubernetes API defaults.

Generic subset matching is insufficient for a frozen Pod: an extra security,
mount, selector or resource field changes what the admitted workload can do.
Unknown defaulting fails closed until qualified against the installed API.
"""
from __future__ import annotations

import copy
from typing import Any

from kubernetes.utils.quantity import parse_quantity

_POD = ("spec", "template", "spec")


def _default(key: str, value: Any, expected: dict[str, Any], path: tuple[str, ...]) -> bool:
    defaults: dict[str, Any] = {}
    if path == ():
        return key == "status" and isinstance(value, dict)
    if path == ("metadata",):
        return key in {"uid", "resourceVersion", "creationTimestamp", "generation", "managedFields"}
    if path == ("spec", "template", "metadata"):
        return key == "creationTimestamp" and value is None
    if path == ("spec",):
        defaults = {"completionMode": "NonIndexed", "suspend": False, "manualSelector": False,
                    "podReplacementPolicy": "TerminatingOrFailed"}
    elif path == _POD:
        defaults = {"dnsPolicy": "ClusterFirst", "schedulerName": "default-scheduler",
                    "serviceAccount": expected.get("serviceAccountName"),
                    "hostNetwork": False, "hostPID": False, "hostIPC": False}
    elif path in {(*_POD, "containers", "*"), (*_POD, "initContainers", "*")}:
        defaults = {"terminationMessagePath": "/dev/termination-log", "terminationMessagePolicy": "File",
                    "imagePullPolicy": "IfNotPresent"}
    elif path[-2:] == ("volumeMounts", "*"):
        defaults = {"readOnly": False, "mountPropagation": "None"}
    elif path[-3:] in {("volumes", "*", "secret"), ("volumes", "*", "configMap"), ("volumes", "*", "projected")}:
        defaults = {"defaultMode": 420}
    elif path[-1:] == ("fieldRef",):
        defaults = {"apiVersion": "v1"}
    return key in defaults and type(value) is type(defaults[key]) and value == defaults[key]


def _omitted(key: str, value: Any, actual: dict[str, Any], path: tuple[str, ...]) -> bool:
    # Go's non-pointer, omitempty booleans disappear even when explicitly sent.
    # Do NOT generalize this to pointer security flags (SA tokens, privilege).
    return (value == [] or (value is False and (
                (path == _POD and key in {"hostNetwork", "hostPID", "hostIPC"})
                or (path[-2:] == ("volumeMounts", "*") and key == "readOnly")))
            or (path[-2:] == ("env", "*") and key == "value" and value == "" and "valueFrom" not in actual))


def _same(actual: Any, expected: Any, path: tuple[str, ...] = ()) -> bool:
    if isinstance(expected, dict):
        return (isinstance(actual, dict)
            and all(_default(key, actual[key], expected, path) for key in actual.keys() - expected.keys())
            and all((_same(actual[key], value, (*path, key)) if key in actual else _omitted(key, value, actual, path))
                for key, value in expected.items()))
    if isinstance(expected, list):
        return isinstance(actual, list) and len(actual) == len(expected) and all(
            _same(a, b, (*path, "*")) for a, b in zip(actual, expected, strict=True))
    if (path[-1:] == ("sizeLimit",) or path[-2:-1] in {("requests",), ("limits",)}):
        try:
            return isinstance(actual, str) and isinstance(expected, str) and parse_quantity(actual) == parse_quantity(expected)
        except (ValueError, TypeError):
            return False
    return type(actual) is type(expected) and actual == expected


def matches_frozen_workload(actual: dict[str, Any], expected: dict[str, Any]) -> bool:
    """Compare exact Job/ConfigMap content, allowing only known server decoration."""
    try:
        value = copy.deepcopy(actual)
        if expected["kind"] == "Job":
            uid, name = value["metadata"]["uid"], expected["metadata"]["name"]
            spec = value["spec"]
            selector = spec.pop("selector", None)
            if selector is not None and selector != {"matchLabels": {"batch.kubernetes.io/controller-uid": uid}}:
                return False
            labels = spec["template"]["metadata"]["labels"]
            for key, wanted in {"controller-uid": uid, "batch.kubernetes.io/controller-uid": uid,
                                "job-name": name, "batch.kubernetes.io/job-name": name}.items():
                if key not in expected["spec"]["template"]["metadata"].get("labels", {}):
                    if key in labels and labels.pop(key) != wanted:
                        return False
        return _same(value, expected)
    except (KeyError, TypeError, AttributeError):
        return False
