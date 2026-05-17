package ironflow

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultSignatureTolerance is the default tolerance for signature timestamp (5 minutes).
	DefaultSignatureTolerance = 5 * time.Minute
)

// SignatureParams contains parsed signature components.
type SignatureParams struct {
	Timestamp  int64
	Signatures map[string]string
}

// ParseSignature parses the X-Ironflow-Signature header.
func ParseSignature(header string) (*SignatureParams, error) {
	if header == "" {
		return nil, ErrMissingSignature
	}

	params := &SignatureParams{
		Signatures: make(map[string]string),
	}

	parts := strings.Split(header, ",")
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}

		key, value := kv[0], kv[1]
		if key == "t" {
			ts, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp: %w", err)
			}
			params.Timestamp = ts
		} else {
			params.Signatures[key] = value
		}
	}

	if params.Timestamp == 0 {
		return nil, fmt.Errorf("missing timestamp in signature")
	}

	return params, nil
}

// ComputeSignature computes the expected signature for a payload.
func ComputeSignature(payload, secret string, timestamp int64) string {
	message := fmt.Sprintf("%d.%s", timestamp, payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

// SignPayload generates a signature header for a payload.
func SignPayload(payload, secret string) string {
	timestamp := time.Now().Unix()
	signature := ComputeSignature(payload, secret, timestamp)
	return fmt.Sprintf("t=%d,v1=%s", timestamp, signature)
}

// VerifySignature verifies a webhook signature.
//
// Returns nil if the signature is valid, or an error otherwise.
func VerifySignature(payload, signature, secret string, tolerance time.Duration) error {
	if signature == "" {
		return ErrMissingSignature
	}

	if secret == "" {
		return fmt.Errorf("missing signing secret")
	}

	params, err := ParseSignature(signature)
	if err != nil {
		return err
	}

	// Check timestamp tolerance
	now := time.Now().Unix()
	age := now - params.Timestamp
	if age < 0 {
		age = -age
	}

	if age > int64(tolerance.Seconds()) {
		return fmt.Errorf("%w: signature is %ds old (max %ds)", ErrSignatureExpired, age, int64(tolerance.Seconds()))
	}

	// Get the v1 signature
	providedSig, ok := params.Signatures["v1"]
	if !ok {
		return fmt.Errorf("missing v1 signature")
	}

	// Compute expected signature
	expectedSig := ComputeSignature(payload, secret, params.Timestamp)

	// Compare using constant-time comparison
	providedBytes, err := hex.DecodeString(providedSig)
	if err != nil {
		return ErrInvalidSignature
	}

	expectedBytes, err := hex.DecodeString(expectedSig)
	if err != nil {
		return ErrInvalidSignature
	}

	if !hmac.Equal(providedBytes, expectedBytes) {
		return ErrInvalidSignature
	}

	return nil
}

// IsValidSignature checks if a signature is valid, returning a boolean.
func IsValidSignature(payload, signature, secret string, tolerance time.Duration) bool {
	return VerifySignature(payload, signature, secret, tolerance) == nil
}
