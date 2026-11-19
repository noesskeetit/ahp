package triggers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// canonicalWebhookMessage builds the canonical string signed for an inbound
// webhook delivery. It binds the trigger id and the caller message id into the
// MAC so a valid signature for one trigger or one message id cannot be replayed
// against another. The raw body is hashed (not inlined) to keep the canonical
// form bounded.
func canonicalWebhookMessage(triggerID, messageID string, body []byte) string {
	sum := sha256.Sum256(body)
	return triggerID + "\n" + messageID + "\n" + hex.EncodeToString(sum[:])
}

// SignWebhook returns the hex HMAC-SHA256 of the canonical message under the
// shared secret. Exported so the SDK and tests can produce a valid signature
// without duplicating the canonicalisation rules.
func SignWebhook(secret []byte, canonical string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

// verifyWebhookSignature checks a hex HMAC signature in constant time. A
// malformed signature hex is treated as a non-match rather than an error, so a
// caller cannot distinguish malformed from incorrect via error channels.
func verifyWebhookSignature(secret []byte, canonical, signature string) bool {
	got, err := hex.DecodeString(signature)
	if err != nil {
		return false
	}
	want, err := hex.DecodeString(SignWebhook(secret, canonical))
	if err != nil {
		return false
	}
	return hmac.Equal(got, want)
}

// SignWebhookDelivery signs the exact bytes forwarded by the public edge.
func SignWebhookDelivery(secret []byte, triggerID, messageID string, body []byte) string {
	return SignWebhook(secret, canonicalWebhookMessage(triggerID, messageID, body))
}
