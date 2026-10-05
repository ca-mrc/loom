package main

import (
	"net/url"
	"testing"
)

// #2310: installed agents in the task sandbox share the Pod's loopback and so
// can reach the model broker. The broker must forward only canonical model
// routes; it is never a general proxy to the Gateway's control endpoints.
func TestBrokerForwardsOnlyCanonicalModelRoutes(t *testing.T) {
	cases := map[string]bool{
		"/v1/chat/completions":                          true,
		"/openai/v1/responses":                          true,
		"/v1beta/models/gemini-2.5-pro:generateContent": true,
		"/google/v1beta/models/x:streamGenerateContent": true,
		"/internal/service-execution/token":             false,
		"/v1/models":                                    false,
		// Traversal through the prefix-matched Gemini routes.
		"/v1beta/models/../../internal/service-execution/token":        false,
		"/google/v1beta/models/x/../../../internal/service-execution/": false,
		"/v1beta/models//x:generateContent":                            false,
		"/v1/chat/completions/":                                        false,
		"/v1/chat/completions/.":                                       false,
	}
	for path, want := range cases {
		if got := allowedGatewayRequest("POST", &url.URL{Path: path}); got != want {
			t.Errorf("%s: allowed=%v", path, got)
		}
	}
	if allowedGatewayRequest("GET", &url.URL{Path: "/v1/chat/completions"}) {
		t.Error("non-POST model route allowed")
	}
	// Percent-encoded segments are never canonical.
	encoded, err := url.Parse("http://broker/v1beta/models/%2e%2e/%2e%2e/internal/service-execution/token")
	if err != nil {
		t.Fatal(err)
	}
	if allowedGatewayRequest("POST", encoded.JoinPath()) || allowedGatewayRequest("POST", encoded) {
		t.Error("encoded traversal allowed")
	}
}
