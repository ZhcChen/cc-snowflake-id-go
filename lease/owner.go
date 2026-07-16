package lease

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// NewOwnerID 生成一个适合租约 owner_id 的进程级唯一标识。
func NewOwnerID(serviceName string) (string, error) {
	serviceName = strings.TrimSpace(serviceName)
	if serviceName == "" {
		return "", fmt.Errorf("%w: service name is required", ErrInvalidLeaseConfig)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("idgen: get hostname: %w", err)
	}
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		hostname = "unknown-host"
	}

	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", fmt.Errorf("idgen: generate owner random: %w", err)
	}
	return fmt.Sprintf("%s:%s:%d:%s", serviceName, hostname, os.Getpid(), hex.EncodeToString(randomBytes)), nil
}

// RedactOwnerID 返回适合日志输出的脱敏 owner_id 表示。
// 租约存储和租约归属判断仍必须使用原始 owner_id。
func RedactOwnerID(ownerID string) string {
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" {
		return ""
	}
	serviceName := "unknown-service"
	parts := strings.Split(ownerID, ":")
	if len(parts) >= 4 {
		serviceName = strings.TrimSpace(parts[0])
	}
	if serviceName == "" {
		serviceName = "unknown-service"
	}
	sum := sha256.Sum256([]byte(ownerID))
	fingerprint := hex.EncodeToString(sum[:])[:12]
	return fmt.Sprintf("%s:<redacted>:%s", serviceName, fingerprint)
}
