package main

import (
	"errors"
	"io"
	"math"
	"math/rand"
	"net"
	"strings"
	"syscall"
	"time"
)

type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

func ShouldRetry(statusCode int, body []byte) (bool, string) {
	switch statusCode {
	case 400:
		bodyStr := string(body)
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
