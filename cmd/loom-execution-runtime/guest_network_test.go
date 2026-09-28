package main

import (
	"context"
	"io"
	"net/http"
	"testing"
)

func TestGuestEgressUsesStableLoopbackAndRetainsAuthority(t *testing.T) {
	b := &workloadBroker{}
	proxy, stop, err := b.startGuestTaskEgress(context.Background(), &webAllowlist{Kind: "web-allowlist", Destinations: []webDestination{{Host: "packages.example.org", Protocol: "https"}}}, "digest", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if proxy != "http://127.0.0.1:18791" {
		t.Fatalf("wrong guest proxy %s", proxy)
	}
	// No active phase: a static guest daemon endpoint conveys no new authority.
	request, _ := http.NewRequest("CONNECT", proxy, nil)
	request.Host = "packages.example.org:443"
	response, err := (&http.Client{}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 504 || response.Header.Get("X-Loom-Egress-Error") != "task_egress_deadline" {
		t.Fatalf("inactive phase permitted: %d", response.StatusCode)
	}
}
