package main

import (
 "os"
 "path/filepath"
 "testing"
)

func TestGuestPayloadMaterializationIsBoundedAndExclusive(t *testing.T){
 source:=t.TempDir()
 if err:=os.Mkdir(filepath.Join(source,"bin"),0755);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(source,"bin/runtime"),[]byte("binary"),0755);err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(source,"kernel"),[]byte("kernel"),0644);err!=nil{t.Fatal(err)}
 target:=filepath.Join(t.TempDir(),"guest")
 if err:=materializeGuestPayload(source,target,12);err!=nil{t.Fatal(err)}
 data,err:=os.ReadFile(filepath.Join(target,"bin/runtime"));if err!=nil||string(data)!="binary"{t.Fatalf("%s %v",data,err)}
 if info,err:=os.Stat(filepath.Join(target,"bin/runtime"));err!=nil || info.Mode().Perm()!=0555{t.Fatalf("%v %v",info,err)}
 if err:=materializeGuestPayload(source,target,12);err==nil{t.Fatal("overwrote existing payload")}
 tooSmall:=filepath.Join(t.TempDir(),"guest")
 if err:=materializeGuestPayload(source,tooSmall,11);err==nil{t.Fatal("ignored payload budget")}
 if _,err:=os.Stat(tooSmall);!os.IsNotExist(err){t.Fatalf("partial payload retained: %v",err)}
}

func TestGuestPayloadRefusesLinksAndPreservesForeignState(t *testing.T){
 source:=t.TempDir();foreign:=t.TempDir()
 if err:=os.WriteFile(filepath.Join(foreign,"keep"),[]byte("foreign"),0644);err!=nil{t.Fatal(err)}
 target:=filepath.Join(t.TempDir(),"guest")
 if err:=os.Symlink(foreign,target);err!=nil{t.Fatal(err)}
 if err:=materializeGuestPayload(source,target,1024);err==nil{t.Fatal("accepted destination link")}
 if data,err:=os.ReadFile(filepath.Join(foreign,"keep"));err!=nil||string(data)!="foreign"{t.Fatalf("changed foreign state %s %v",data,err)}
 if err:=os.Symlink(filepath.Join(foreign,"keep"),filepath.Join(source,"link"));err!=nil{t.Fatal(err)}
 fresh:=filepath.Join(t.TempDir(),"guest")
 if err:=materializeGuestPayload(source,fresh,1024);err==nil{t.Fatal("followed source link")}
 if _,err:=os.Stat(fresh);!os.IsNotExist(err){t.Fatalf("partial payload retained %v",err)}
}
