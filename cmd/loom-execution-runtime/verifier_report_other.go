//go:build !linux

package main

import "fmt"

func prepareVerifierReport(workspace, report string) error {
	return fmt.Errorf("native verifier report preparation requires Linux")
}
