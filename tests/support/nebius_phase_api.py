"""External Kubernetes double shared by management and participant tests."""
import copy
from dataclasses import replace
from uuid import uuid4


class PhaseAPI:
    """External API double; renderer, state, comparisons and writes remain real."""

    def __init__(self, binding):
        self.binding = binding
        self.resources = {}
        self.creates = []
        self.failure = None
        self.default_change = None

    @staticmethod
    def key(doc):
        return doc["kind"] + ":" + doc["metadata"]["name"]

    def verify_identity(self, binding):
        if binding != self.binding:
            raise RuntimeError("private-identity-detail")

    def get_resource(self, doc):
        return copy.deepcopy(self.resources.get(self.key(doc)))

    def get_database_claim(self):
        return copy.deepcopy(self.resources.get("PersistentVolumeClaim:data-loom-postgres-0"))

    def get_database_volume(self):
        claim = self.get_database_claim()
        name = (claim or {}).get("spec", {}).get("volumeName")
        return copy.deepcopy(self.resources.get("PersistentVolume:" + str(name)))

    def default_resource(self, doc):
        value = copy.deepcopy(doc)
        if doc["kind"] == "Job":
            uid = str(uuid4())
            value["metadata"]["uid"] = uid
            value["spec"]["selector"] = {"matchLabels": {"batch.kubernetes.io/controller-uid": uid}}
            value["spec"]["template"]["metadata"]["labels"].update({
                "controller-uid": uid, "batch.kubernetes.io/controller-uid": uid,
                "job-name": doc["metadata"]["name"], "batch.kubernetes.io/job-name": doc["metadata"]["name"],
            })
        if self.default_change:
            self.default_change(value)
        return value

    def create_resource(self, doc):
        self.creates.append(self.key(doc))
        if self.failure == "before":
            raise OSError("private-request-detail")
        value = self.default_resource(doc)
        value["metadata"].setdefault("uid", str(uuid4()))
        value["metadata"]["resourceVersion"] = "1"
        self.resources[self.key(doc)] = value
        if self.failure == "after":
            raise OSError("private-response-detail")
        if self.failure == "identity":
            self.binding = replace(self.binding, namespace_uid=str(uuid4()))
