package lease

import (
	"strings"
	"testing"
)

func TestNewOwnerID(t *testing.T) {
	ownerID, err := NewOwnerID("api")
	if err != nil {
		t.Fatalf("NewOwnerID() error = %v", err)
	}
	parts := strings.Split(ownerID, ":")
	if len(parts) != 4 {
		t.Fatalf("ownerID = %q, want 4 parts", ownerID)
	}
	if parts[0] != "api" || parts[1] == "" || parts[2] == "" || len(parts[3]) != 16 {
		t.Fatalf("ownerID parts = %#v", parts)
	}
}

func TestNewOwnerIDRejectsBlankService(t *testing.T) {
	_, err := NewOwnerID(" ")
	if err == nil {
		t.Fatalf("NewOwnerID() error = nil, want validation error")
	}
}

func TestRedactOwnerIDKeepsServiceAndStableFingerprint(t *testing.T) {
	raw := "api:host-a:1234:abcdef0123456789"
	redacted := RedactOwnerID(raw)
	if redacted == "" || redacted == raw {
		t.Fatalf("RedactOwnerID(%q) = %q, want non-empty redacted value", raw, redacted)
	}
	parts := strings.Split(redacted, ":")
	if len(parts) != 3 || parts[0] != "api" || parts[1] != "<redacted>" || len(parts[2]) != 12 {
		t.Fatalf("redacted owner parts = %#v", parts)
	}
	for _, leaked := range []string{"host-a", "1234", "abcdef0123456789"} {
		if strings.Contains(redacted, leaked) {
			t.Fatalf("redacted owner %q leaked %q", redacted, leaked)
		}
	}
	if again := RedactOwnerID(raw); again != redacted {
		t.Fatalf("RedactOwnerID should be stable: first=%q second=%q", redacted, again)
	}
	if other := RedactOwnerID("api:host-b:5678:abcdef0123456789"); other == redacted {
		t.Fatalf("different owner should have different fingerprint: %q", other)
	}
}

func TestRedactOwnerIDDoesNotExposeUnstructuredOwner(t *testing.T) {
	for _, raw := range []string{"legacy-host-pid-secret", "legacy-host:pid-secret"} {
		t.Run(raw, func(t *testing.T) {
			redacted := RedactOwnerID(raw)
			if strings.Contains(redacted, raw) || strings.Contains(redacted, "legacy-host") {
				t.Fatalf("redacted owner %q leaked unstructured owner %q", redacted, raw)
			}
			parts := strings.Split(redacted, ":")
			if len(parts) != 3 || parts[0] != "unknown-service" || parts[1] != "<redacted>" || len(parts[2]) != 12 {
				t.Fatalf("redacted owner parts = %#v", parts)
			}
		})
	}
	if RedactOwnerID(" ") != "" {
		t.Fatalf("blank owner should redact to empty string")
	}
}
