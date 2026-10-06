package main

// Cacheable harness installs (#2310).
//
// The trusted runtime, never a sandbox process, moves cache archives: it
// fetches an entry into the controller workspace before the setup phase and
// stores the archive the setup phase produced afterwards. The Gateway derives
// the object key from the lease (team, task image, install identity), so a Pod
// cannot name or read another team's or image's entry. Any cache failure only
// means a fresh install.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

const maxSetupCacheBytes = 256 * 1024 * 1024

var (
	setupCacheDirectory = filepath.Join(".loom", "harness-cache")
	setupCacheRoot      = regexp.MustCompile(`^/[A-Za-z0-9._/-]{1,255}$`)
	errSetupCacheMiss   = errors.New("setup cache miss")
)

type setupCache struct {
	SchemaVersion  string `json:"schema_version"`
	IdentitySHA256 string `json:"identity_sha256"`
	InstallRoot    string `json:"install_root"`
	MaxBytes       int64  `json:"max_bytes"`
}

func (c *setupCache) validate() error {
	if c.SchemaVersion != "loom.runtime-setup-cache.v1" || !sha256Value.MatchString(c.IdentitySHA256) ||
		!setupCacheRoot.MatchString(c.InstallRoot) || c.MaxBytes <= 0 || c.MaxBytes > maxSetupCacheBytes {
		return fmt.Errorf("invalid setup cache")
	}
	return nil
}

// Paths inside the controller workspace that the setup phase reads and writes.
func setupCacheRestorePath(workspace string) string {
	return filepath.Join(workspace, setupCacheDirectory, "restore.tar.gz")
}

func setupCacheStorePath(workspace string) string {
	return filepath.Join(workspace, setupCacheDirectory, "store.tar.gz")
}

// fetchSetupCache places a verified cache entry at the restore path. A miss
// or any failure leaves no file, so the setup phase installs normally.
func (b *workloadBroker) fetchSetupCache(ctx context.Context, p plan, workspace string) error {
	if p.SetupCache == nil {
		return nil
	}
	directory := filepath.Join(workspace, setupCacheDirectory)
	if err := secureInputDirectory(workspace, directory); err != nil {
		return err
	}
	response, err := b.getInput(ctx, b.endpoint("/harness-cache"))
	if err != nil {
		var brokerErr *brokerHTTPError
		if errors.As(err, &brokerErr) && brokerErr.statusCode == http.StatusNotFound {
			return errSetupCacheMiss
		}
		return err
	}
	defer response.Body.Close()
	expected := response.Header.Get("X-Loom-Content-SHA256")
	size, err := strconv.ParseInt(response.Header.Get("Content-Length"), 10, 64)
	if err != nil || size <= 0 || size > p.SetupCache.MaxBytes || !sha256Value.MatchString(expected) {
		return fmt.Errorf("setup cache response invalid")
	}
	temporary := setupCacheRestorePath(workspace) + ".partial"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	digest := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, digest), io.LimitReader(response.Body, size+1))
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil || written != size ||
		"sha256:"+hex.EncodeToString(digest.Sum(nil)) != expected {
		_ = os.Remove(temporary)
		return fmt.Errorf("setup cache entry failed verification")
	}
	return os.Rename(temporary, setupCacheRestorePath(workspace))
}

// storeSetupCache uploads the archive a fresh install produced, if any.
func (b *workloadBroker) storeSetupCache(ctx context.Context, p plan, workspace string) error {
	if p.SetupCache == nil {
		return nil
	}
	path := setupCacheStorePath(workspace)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > p.SetupCache.MaxBytes {
		return fmt.Errorf("setup cache archive invalid")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	return retryOperationIf(ctx, func() error {
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, b.endpoint("/harness-cache"), bytes.NewReader(payload))
		if err != nil {
			return err
		}
		b.identityHeaders(request)
		if err := b.authorizePodRequest(request); err != nil {
			return err
		}
		request.ContentLength = int64(len(payload))
		request.Header.Set("Content-Type", "application/gzip")
		request.Header.Set("X-Loom-Content-SHA256", "sha256:"+hex.EncodeToString(digest[:]))
		response, err := b.client.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			return &brokerHTTPError{statusCode: response.StatusCode, body: string(bytes.TrimSpace(body))}
		}
		return nil
	}, workloadIdentityTemporarilyUnavailable)
}

// runSetupCacheStep bounds a cache transfer and reports, never fails, a trial.
func runSetupCacheStep(parent context.Context, operation string, step func(context.Context) error) {
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	if err := step(ctx); err != nil && !errors.Is(err, errSetupCacheMiss) {
		fmt.Fprintf(os.Stderr, "harness setup cache %s skipped: %v\n", operation, err)
	}
}
