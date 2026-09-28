package guestchannel

import (
 "context"
 "fmt"
 "io"
 "net"
 "net/http"
 "sync"
 "testing"
 "time"
)

// Exercise concurrent health, long execution and artifact streams on one
// virtio-serial byte stream. A long task must not starve incarnation probes.
func TestConcurrentHTTPStreams(t *testing.T) {
 left, right := net.Pipe()
 serverReady := make(chan net.Listener, 1)
 go func() {
  listener, err := Serve(right)
  if err != nil { t.Error(err); return }
  serverReady <- listener
 }()
 client, err := Connect(context.Background(), left)
 if err != nil { t.Fatal(err) }
 defer client.Close()
 listener := <-serverReady
 defer listener.Close()
 release := make(chan struct{})
 started := make(chan struct{})
 server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
  if r.URL.Path == "/exec" { close(started); <-release }
  _, _ = fmt.Fprint(w, r.URL.Path)
 })}
 defer server.Close()
 go func() { _ = server.Serve(listener) }()
 transport := &http.Transport{DialContext: client.DialContext}
 defer transport.CloseIdleConnections()
 httpClient := &http.Client{Transport: transport, Timeout: 3*time.Second}
 var workers sync.WaitGroup
 workers.Add(1)
 go func(){ defer workers.Done(); response, err := httpClient.Get("http://guest/exec"); if err != nil { t.Error(err); return }; response.Body.Close() }()
 <-started
 for i:=0; i<8; i++ {
  workers.Add(1)
  go func(){ defer workers.Done(); response, err := httpClient.Get("http://guest/health"); if err != nil { t.Error(err); return }; defer response.Body.Close(); data,_:=io.ReadAll(response.Body); if string(data)!="/health" {t.Errorf("response: %s",data)} }()
 }
 // Wait for the independent probes while the exec request is still blocked.
 probe, err := httpClient.Get("http://guest/health")
 if err != nil { t.Fatal(err) }
 probe.Body.Close()
 close(release)
 workers.Wait()
}

func TestHandshakeCancellationClosesTransport(t *testing.T) {
 left, right := net.Pipe()
 defer right.Close()
 ctx,cancel:=context.WithCancel(context.Background())
 done:=make(chan error,1)
 go func(){ _,err:=Connect(ctx,left); done<-err }()
 cancel()
 select {
 case err:=<-done: if err==nil {t.Fatal("cancelled connect succeeded")}
 case <-time.After(time.Second):t.Fatal("cancelled handshake still blocked")
 }
 _=right.SetWriteDeadline(time.Now().Add(time.Second))
 if _,err:=right.Write([]byte("LOOMRPC1"));err==nil {t.Fatal("cancelled transport left open")}
}

func TestDisconnectInvalidatesExistingClient(t *testing.T) {
 left,right:=net.Pipe()
 ready:=make(chan net.Listener,1)
 go func(){ listener,err:=Serve(right); if err!=nil {t.Error(err);return}; ready<-listener }()
 client,err:=Connect(context.Background(),left)
 if err!=nil {t.Fatal(err)}
 defer client.Close()
 listener:=<-ready
 listener.Close()
 ctx,cancel:=context.WithTimeout(context.Background(),time.Second)
 defer cancel()
 for {
  stream,err:=client.DialContext(ctx,"tcp","ignored")
  if err!=nil {return}
  stream.Close()
  select {case <-ctx.Done():t.Fatal("disconnected client remained usable");default:}
 }
}
