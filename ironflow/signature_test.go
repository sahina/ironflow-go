package ironflow

import (
	"strings"
	"testing"
	"time"
)

func TestComputeSignature(t *testing.T) {
	secret := "whsec_test_secret_key_12345"
	payload := `{"event":"test.event","data":{"id":"123"}}`
	timestamp := int64(1704067200)

	t.Run("produces consistent signature", func(t *testing.T) {
		sig1 := ComputeSignature(payload, secret, timestamp)
		sig2 := ComputeSignature(payload, secret, timestamp)

		if sig1 != sig2 {
			t.Errorf("signatures should match: %s != %s", sig1, sig2)
		}

		// Should be 64 hex characters (SHA-256)
		if len(sig1) != 64 {
			t.Errorf("signature should be 64 characters, got %d", len(sig1))
		}
	})

	t.Run("different payloads produce different signatures", func(t *testing.T) {
		sig1 := ComputeSignature(payload, secret, timestamp)
		sig2 := ComputeSignature(`{"different":"payload"}`, secret, timestamp)

		if sig1 == sig2 {
			t.Error("different payloads should produce different signatures")
		}
	})

	t.Run("different secrets produce different signatures", func(t *testing.T) {
		sig1 := ComputeSignature(payload, secret, timestamp)
		sig2 := ComputeSignature(payload, "different_secret", timestamp)

		if sig1 == sig2 {
			t.Error("different secrets should produce different signatures")
		}
	})

	t.Run("different timestamps produce different signatures", func(t *testing.T) {
		sig1 := ComputeSignature(payload, secret, 1704067200)
		sig2 := ComputeSignature(payload, secret, 1704067201)

		if sig1 == sig2 {
			t.Error("different timestamps should produce different signatures")
		}
	})
}

func TestSignPayload(t *testing.T) {
	secret := "whsec_test_secret_key_12345"
	payload := `{"event":"test.event"}`

	t.Run("generates valid signature header", func(t *testing.T) {
		header := SignPayload(payload, secret)

		// Should contain timestamp and v1 signature
		if len(header) == 0 {
			t.Error("header should not be empty")
		}

		// Parse and verify format
		parts, err := ParseSignature(header)
		if err != nil {
			t.Fatalf("failed to parse signature: %v", err)
		}
		if parts.Timestamp == 0 {
			t.Error("header should contain timestamp")
		}

		sig, ok := parts.Signatures["v1"]
		if !ok || len(sig) != 64 {
			t.Error("header should contain valid v1 signature")
		}
	})
}

func TestVerifySignature(t *testing.T) {
	secret := "whsec_test_secret_key_12345"
	payload := `{"event":"test.event","data":{"id":"123"}}`

	// Create a valid signature with current timestamp
	now := time.Now().Unix()
	sig := ComputeSignature(payload, secret, now)
	validHeader := formatSignatureHeader(now, sig)

	t.Run("verifies valid signature", func(t *testing.T) {
		err := VerifySignature(payload, validHeader, secret, DefaultSignatureTolerance)
		if err != nil {
			t.Errorf("should verify valid signature: %v", err)
		}
	})

	t.Run("rejects missing signature", func(t *testing.T) {
		err := VerifySignature(payload, "", secret, DefaultSignatureTolerance)
		if err == nil {
			t.Error("should reject missing signature")
		}
	})

	t.Run("rejects missing secret", func(t *testing.T) {
		err := VerifySignature(payload, validHeader, "", DefaultSignatureTolerance)
		if err == nil {
			t.Error("should reject missing secret")
		}
	})

	t.Run("rejects invalid format - missing timestamp", func(t *testing.T) {
		err := VerifySignature(payload, "v1=abc123", secret, DefaultSignatureTolerance)
		if err == nil {
			t.Error("should reject missing timestamp")
		}
	})

	t.Run("rejects invalid format - missing v1 signature", func(t *testing.T) {
		err := VerifySignature(payload, "t=1704067200", secret, DefaultSignatureTolerance)
		if err == nil {
			t.Error("should reject missing v1 signature")
		}
	})

	t.Run("rejects expired signature", func(t *testing.T) {
		oldTimestamp := time.Now().Add(-10 * time.Minute).Unix()
		oldSig := ComputeSignature(payload, secret, oldTimestamp)
		oldHeader := formatSignatureHeader(oldTimestamp, oldSig)

		err := VerifySignature(payload, oldHeader, secret, 5*time.Minute)
		if err == nil {
			t.Error("should reject expired signature")
		}
	})

	t.Run("accepts signature within tolerance", func(t *testing.T) {
		// 4 minutes ago
		pastTimestamp := time.Now().Add(-4 * time.Minute).Unix()
		pastSig := ComputeSignature(payload, secret, pastTimestamp)
		pastHeader := formatSignatureHeader(pastTimestamp, pastSig)

		err := VerifySignature(payload, pastHeader, secret, 5*time.Minute)
		if err != nil {
			t.Errorf("should accept signature within tolerance: %v", err)
		}
	})

	t.Run("rejects invalid signature", func(t *testing.T) {
		// Use a completely different but valid hex string to ensure it doesn't match
		invalidSig := strings.Repeat("00", 32) // 64 hex chars of zeros
		if invalidSig == sig {
			invalidSig = strings.Repeat("ff", 32) // fallback if sig was all zeros
		}
		invalidHeader := formatSignatureHeader(now, invalidSig)

		err := VerifySignature(payload, invalidHeader, secret, DefaultSignatureTolerance)
		if err == nil {
			t.Error("should reject invalid signature")
		}
	})

	t.Run("rejects tampered payload", func(t *testing.T) {
		err := VerifySignature(`{"tampered":"payload"}`, validHeader, secret, DefaultSignatureTolerance)
		if err == nil {
			t.Error("should reject tampered payload")
		}
	})
}

func TestIsValidSignature(t *testing.T) {
	secret := "whsec_test_secret_key_12345"
	payload := `{"event":"test.event"}`

	t.Run("returns true for valid signature", func(t *testing.T) {
		header := SignPayload(payload, secret)
		if !IsValidSignature(payload, header, secret, DefaultSignatureTolerance) {
			t.Error("should return true for valid signature")
		}
	})

	t.Run("returns false for invalid signature", func(t *testing.T) {
		if IsValidSignature(payload, "invalid", secret, DefaultSignatureTolerance) {
			t.Error("should return false for invalid signature")
		}
	})

	t.Run("returns false for empty signature", func(t *testing.T) {
		if IsValidSignature(payload, "", secret, DefaultSignatureTolerance) {
			t.Error("should return false for empty signature")
		}
	})
}

func TestParseSignature(t *testing.T) {
	t.Run("extracts timestamp from valid signature", func(t *testing.T) {
		timestamp := int64(1704067200)
		header := formatSignatureHeader(timestamp, repeatChar('a', 64))

		params, err := ParseSignature(header)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if params.Timestamp != timestamp {
			t.Errorf("expected timestamp %d, got %d", timestamp, params.Timestamp)
		}
	})

	t.Run("returns error for empty signature", func(t *testing.T) {
		_, err := ParseSignature("")
		if err == nil {
			t.Error("should return error for empty signature")
		}
	})

	t.Run("returns error for missing timestamp", func(t *testing.T) {
		_, err := ParseSignature("v1=abc123")
		if err == nil {
			t.Error("should return error for missing timestamp")
		}
	})
}

// Helper to create a repeated character string
func repeatChar(c byte, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}
	return string(b)
}

// Helper to format signature header
func formatSignatureHeader(timestamp int64, sig string) string {
	return "t=" + itoa(timestamp) + ",v1=" + sig
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
