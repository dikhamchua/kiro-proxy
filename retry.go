package main

import (
	"errors"
	"io"
	"math"
	"math/rand"
	"net"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

// resetAfterPattern matches kiro upstream's "(reset after 26s)" or "(reset after 2m)" suffix.
var resetAfterPattern = regexp.MustCompile(`reset after (\d+)([sm])`)

// ParseResetAfter extracts the reset duration from a kiro error body.
// Returns 0 if no reset hint is present.
func ParseResetAfter(body []byte) time.Duration {
	return parseResetAfterString(string(body))
}

func parseResetAfterString(s string) time.Duration {
	m := resetAfterPattern.FindStringSubmatch(s)
	if len(m) != 3 {
		return 0
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n <= 0 {
		return 0
	}
	switch m[2] {
	case "s":
		return time.Duration(n) * time.Second
	case "m":
		return time.Duration(n) * time.Minute
	}
	return 0
}

// parseResetFromError extracts a reset hint embedded in an error message.
func parseResetFromError(err error) time.Duration {
	if err == nil {
		return 0
	}
	return parseResetAfterString(err.Error())
}

func ShouldRetry(statusCode int, body []byte) (bool, string) {
	bodyStr := string(body)

	// Kiro upstream sometimes returns 400/403 with "(reset after Xs)" — that's
	// a transient per-model quota, not a real client error. Treat as retryable.
	if reset := ParseResetAfter(body); reset > 0 {
		return true, "upstream model rate-limited (reset after " + reset.String() + ")"
	}

	switch statusCode {
	case 400:
		if strings.Contains(bodyStr, "Improperly formed request") {
			return true, "improperly formed request - will retry with cleaned model"
		}
		return false, ""
	case 429:
		return true, "rate limited"
	case 502:
		return true, "bad gateway"
	case 503:
		return true, "service unavailable"
	case 504:
		return true, "gateway timeout"
	default:
		return false, ""
	}
}

func CalculateBackoff(attempt int, config RetryConfig) time.Duration {
	delay := float64(config.BaseDelay) * math.Pow(2, float64(attempt-1))
	if delay > float64(config.MaxDelay) {
		delay = float64(config.MaxDelay)
	}
	jitter := rand.Float64() * 0.3 * delay
	return time.Duration(delay + jitter)
}

func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	errMsg := err.Error()
	retryableMessages := []string{
		"stream truncated",
		"stream incomplete",
		"upstream connect",
		"improperly formed request",
		"bad gateway",
		"service unavailable",
		"gateway timeout",
		"rate limited",
		"rate-limited",
		"reset after",
		"connection reset",
		"broken pipe",
		"empty response",
		"invalid response",
	}

	for _, msg := range retryableMessages {
		if strings.Contains(strings.ToLower(errMsg), msg) {
			return true
		}
	}

	return false
}
