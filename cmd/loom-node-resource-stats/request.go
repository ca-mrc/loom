package main

import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
	"strings"
	"time"
)

type request struct {
	SchemaVersion string       `json:"schema_version"`
	Namespace     string       `json:"namespace"`
	Pods          []podRequest `json:"pods"`
}
type podRequest struct {
	UID        string             `json:"uid"`
	Name       string             `json:"name"`
	Containers []containerRequest `json:"containers"`
	Volumes    []volumeRequest    `json:"volumes"`
}
type containerRequest struct {
	Name      string `json:"name"`
	ID        string `json:"container_id"`
	StartedAt string `json:"started_at"`
}
type volumeRequest struct {
	Kind   string `json:"kind,omitempty"`
	Name   string `json:"name"`
	Medium string `json:"medium"`
}

var labelPattern = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$`)
var podPattern = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$`)
var uidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var idPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func decodeRequest(input io.Reader) (request, string) {
	var req request
	body, err := io.ReadAll(io.LimitReader(input, maxPayload+1))
	if err != nil {
		return req, "request_unavailable"
	}
	if len(body) > maxPayload {
		return req, "request_too_large"
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&req) != nil || decoder.Decode(new(any)) != io.EOF {
		return req, "invalid_request"
	}
	if req.SchemaVersion != "loom.node-resource-request.v1" || !validLabel(req.Namespace) || req.Pods == nil || len(req.Pods) > 128 {
		return req, "invalid_request"
	}
	pods := map[string]bool{}
	allIDs := map[string]bool{}
	for i := range req.Pods {
		pod := &req.Pods[i]
		if !uidPattern.MatchString(pod.UID) || !validPodName(pod.Name) || pods[pod.UID] || len(pod.Containers) > 16 || len(pod.Volumes) > 64 {
			return request{}, "invalid_request"
		}
		pods[pod.UID] = true
		names := map[string]bool{}
		for j := range pod.Containers {
			container := &pod.Containers[j]
			id := strings.TrimPrefix(container.ID, "containerd://")
			started, err := time.Parse(time.RFC3339Nano, container.StartedAt)
			if !validLabel(container.Name) || !idPattern.MatchString(id) || names[container.Name] || allIDs[id] || err != nil || started.UnixNano() <= 0 {
				return request{}, "invalid_request"
			}
			container.ID = id
			names[container.Name] = true
			allIDs[id] = true
		}
		volumes := map[string]bool{}
		for _, volume := range pod.Volumes {
			if !validLabel(volume.Name) || volumes[volume.Name] || (volume.Medium != "" && volume.Medium != "Memory") || (volume.Kind != "" && volume.Kind != "empty_dir" && volume.Kind != "config_map" && volume.Kind != "git_repo") || (volume.Medium != "" && volume.Kind != "" && volume.Kind != "empty_dir") {
				return request{}, "invalid_request"
			}
			volumes[volume.Name] = true
		}
	}
	return req, ""
}

func validLabel(value string) bool {
	return len(value) > 0 && len(value) <= 63 && labelPattern.MatchString(value)
}
func validPodName(value string) bool {
	if len(value) == 0 || len(value) > 253 || !podPattern.MatchString(value) {
		return false
	}
	for _, part := range strings.Split(value, ".") {
		if !validLabel(part) {
			return false
		}
	}
	return true
}
