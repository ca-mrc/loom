package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type preparedImageInput struct {
	ImageRef  string `json:"image_ref"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

func (b *workloadBroker) materializePreparedImages(ctx context.Context, p plan, root string) error {
	if p.ExecutionRole != "attempt" {
		return nil
	}
	raw := p.Main.Environment["LOOM_TASK_ENVIRONMENT_JSON"]
	if raw == "" {
		return nil
	}
	var env struct {
		StorageMiB int64 `json:"storage_mb"`
		Compose    *struct {
			Services []struct {
				Name  string          `json:"name"`
				Image string          `json:"image"`
				Build json.RawMessage `json:"build"`
			} `json:"services"`
		} `json:"native_compose"`
	}
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return fmt.Errorf("invalid prepared image environment")
	}
	if env.Compose == nil {
		return nil
	}
	if p.TaskImageMaterializationID == nil || env.StorageMiB <= 0 || env.StorageMiB > 1048576 {
		return fmt.Errorf("prepared images require a grant and storage")
	}
	response, err := b.getInput(ctx, b.endpoint("/inputs/images/manifest"))
	if err != nil {
		return err
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, 32769))
	response.Body.Close()
	if err != nil || len(payload) > 32768 {
		return fmt.Errorf("invalid prepared image manifest")
	}
	var inventory map[string]preparedImageInput
	if err = json.Unmarshal(payload, &inventory); err != nil || len(inventory) != len(env.Compose.Services) {
		return fmt.Errorf("prepared image inventory differs from frozen services")
	}
	var total int64
	for _, service := range env.Compose.Services {
		item, present := inventory[service.Name]
		if !present || !roleName.MatchString(service.Name) || item.ImageRef != service.Image || !digestImage.MatchString(item.ImageRef) ||
			(len(service.Build) != 0 && string(service.Build) != "null") || !sha256Value.MatchString(item.SHA256) || item.SizeBytes <= 0 || item.SizeBytes > 3*1024*1024*1024 {
			return fmt.Errorf("prepared image differs from frozen service")
		}
		total += item.SizeBytes
	}
	if total > env.StorageMiB*1024*1024 {
		return fmt.Errorf("prepared images exceed input storage")
	}
	if err = secureDirectory(root); err != nil {
		return err
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		return fmt.Errorf("prepared image directory is not fresh")
	}
	for _, service := range env.Compose.Services {
		item := inventory[service.Name]
		response, err := b.getInput(ctx, b.endpoint("/inputs/images/"+service.Name))
		if err != nil {
			return err
		}
		destination := filepath.Join(root, service.Name+".tar")
		file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			response.Body.Close()
			return err
		}
		hash := sha256.New()
		count, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, item.SizeBytes+1))
		closeErr := file.Close()
		response.Body.Close()
		if copyErr != nil || closeErr != nil || count != item.SizeBytes || "sha256:"+hex.EncodeToString(hash.Sum(nil)) != item.SHA256 {
			os.Remove(destination)
			return fmt.Errorf("prepared image transport integrity mismatch")
		}
	}
	return nil
}
