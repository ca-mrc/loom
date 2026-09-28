package main

import (
	"context"
	"net"
	"net/http"
 "os"
 "path/filepath"
	"testing"
	"time"

	"github.com/qianyi-sun/loom/internal/guestchannel"
)

func TestGuestListenerServesExistingSandboxAPI(t *testing.T) {
	host, guest := net.Pipe()
	ready := make(chan net.Listener, 1)
	go func() {
		listener, err := sandboxListener("", guest)
		if err != nil {
			t.Error(err)
			return
		}
		ready <- listener
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	channel, err := guestchannel.Connect(ctx, host)
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	server := &http.Server{Handler: (runtimeServer{1024, time.Second}).handler()}
	defer server.Close()
	go func() { _ = server.Serve(<-ready) }()
	client := &http.Client{Transport: &http.Transport{DialContext: channel.DialContext}, Timeout: time.Second}
	defer client.CloseIdleConnections()
	status, result := runExec(t, client, execRequest{Argv: []string{"/bin/sh", "-c", "printf guest-rpc"}})
	if status != 200 || result.Code != 0 || string(result.Stdout) != "guest-rpc" {
		t.Fatalf("%d %#v", status, result)
	}
}

func TestGuestListenerRejectsAmbiguousTransport(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	if _, err := sandboxListener("/tmp/forbidden.sock", left); err == nil {
		t.Fatal("accepted mixed transport")
	}
	if _, err := sandboxListener("", nil); err == nil {
		t.Fatal("accepted missing transport")
	}
}

func TestGuestChannelDiscoveryUsesNamedPortRatherThanOrdinal(t *testing.T) {
 root:=t.TempDir()
 for _,port:=range []string{"vport0p0","vport1p4"} {
  if err:=os.MkdirAll(filepath.Join(root,port),0755);err!=nil{t.Fatal(err)}
  name:="unrelated";if port=="vport1p4" {name="loom.rpc"}
  if err:=os.WriteFile(filepath.Join(root,port,"name"),[]byte(name+"\n"),0644);err!=nil {t.Fatal(err)}
 }
 path,err:=guestChannelDevice(root,"/dev")
 if err!=nil || path!="/dev/vport1p4" {t.Fatalf("%q %v",path,err)}
 if err:=os.WriteFile(filepath.Join(root,"vport0p0/name"),[]byte("loom.rpc\n"),0644);err!=nil{t.Fatal(err)}
 if _,err:=guestChannelDevice(root,"/dev");err==nil {t.Fatal("accepted ambiguous RPC port")}
}
