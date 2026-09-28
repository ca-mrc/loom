package main

import (
 "context"
 "net"
 "net/http"
 "testing"
 "time"

 "github.com/qianyi-sun/loom/internal/guestchannel"
)

func TestGuestListenerServesExistingSandboxAPI(t *testing.T) {
 host,guest:=net.Pipe()
 ready:=make(chan net.Listener,1)
 go func(){ listener,err:=sandboxListener("",guest); if err!=nil {t.Error(err);return}; ready<-listener }()
 ctx,cancel:=context.WithTimeout(context.Background(),time.Second)
 defer cancel()
 channel,err:=guestchannel.Connect(ctx,host)
 if err!=nil {t.Fatal(err)}
 defer channel.Close()
 server:=&http.Server{Handler:(runtimeServer{1024,time.Second}).handler()}
 defer server.Close()
 go func(){_=server.Serve(<-ready)}()
 client:=&http.Client{Transport:&http.Transport{DialContext:channel.DialContext},Timeout:time.Second}
 defer client.CloseIdleConnections()
 status,result:=runExec(t,client,execRequest{Argv:[]string{"/bin/sh","-c","printf guest-rpc"}})
 if status!=200 || result.Code!=0 || string(result.Stdout)!="guest-rpc" {t.Fatalf("%d %#v",status,result)}
}

func TestGuestListenerRejectsAmbiguousTransport(t *testing.T) {
 left,right:=net.Pipe();defer left.Close();defer right.Close()
 if _,err:=sandboxListener("/tmp/forbidden.sock",left);err==nil {t.Fatal("accepted mixed transport")}
 if _,err:=sandboxListener("",nil);err==nil {t.Fatal("accepted missing transport")}
}
