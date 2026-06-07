package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type ProxyHandler struct {
	UpstreamURL string
	MaxRetries  int
	Logger      *log.Logger
	Client      *http.Client
}

var modelSuffixRe = regexp.MustCompile(`\[\d+m\]$`)

func (p *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		p.writeError(w, 400, "failed to read request body")
		return
	}
	r.Body.Close()

	body = p.cleanRequestBody(body)
	isStream := p.isStreamingRequest(body, r)

	var lastErr error
	for attempt := 0; attempt <= p.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := CalculateBackoff(attempt, RetryConfig{
				BaseDelay: 1 * time.Second,
				MaxDelay:  10 * time.Second,
			})
			p.Logger.Printf("[retry] attempt %d/%d after %v", attempt, p.MaxRetries, delay)
			time.Sleep(delay)
		}

		if isStream {
			lastErr = p.handleStreamingBuffered(w, r, body)
		} else {
			lastErr = p.handleNonStreaming(w, r, body)
		}

		if lastErr == nil {
			return
		}

		if !IsRetryableError(lastErr) {
			p.Logger.Printf("[error] non-retryable: %v", lastErr)
			break
		}

		p.Logger.Printf("[error] retryable: %v", lastErr)
	}

	if lastErr != nil {
		p.writeError(w, 502, fmt.Sprintf("upstream failed after %d attempts: %v", p.MaxRetries+1, lastErr))
	}
}

// handleStreamingBuffered buffers the entire SSE stream from upstream,
// validates completeness, then forwards to client. If truncated, returns error for retry.
func (p *ProxyHandler) handleStreamingBuffered(w http.ResponseWriter, r *http.Request, body []byte) error {
	resp, err := p.doUpstreamRequest(r, body)
	if err != nil {
		return fmt.Errorf("upstream connect: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		shouldRetry, reason := ShouldRetry(resp.StatusCode, respBody)
		if shouldRetry {
			return fmt.Errorf("upstream %d: %s", resp.StatusCode, reason)
		}
		p.forwardRaw(w, respBody, resp.Header, resp.StatusCode)
		return nil
	}

	// Read entire response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if len(respBody) == 0 {
		return fmt.Errorf("empty response from upstream")
	}

	// Determine if response is SSE or JSON by looking at content
	bodyStr := strings.TrimSpace(string(respBody))
	isSSE := strings.HasPrefix(bodyStr, "event:") || strings.HasPrefix(bodyStr, "data:")

	if isSSE {
		// Parse SSE events from buffered body
		state := NewStreamState()
		var events []SSEEvent

		scanner := bufio.NewScanner(bytes.NewReader(respBody))
		scanner.Buffer(make([]byte, 4*1024*1024), 4*1024*1024)

		var eventType string
		var dataLines []string

		for scanner.Scan() {
			line := scanner.Text()

			if strings.HasPrefix(line, "event: ") {
				eventType = strings.TrimPrefix(line, "event: ")
				continue
			}
			if strings.HasPrefix(line, "data: ") {
				dataLines = append(dataLines, strings.TrimPrefix(line, "data: "))
				continue
			}
			if line == "" && eventType != "" {
				data := strings.Join(dataLines, "\n")
				state.ProcessEvent(eventType, data)
				events = append(events, SSEEvent{Type: eventType, Data: data})
				eventType = ""
				dataLines = nil
			}
		}

		if state.IsIncomplete() {
			return fmt.Errorf("stream truncated: %d open blocks, message_complete=%v", state.OpenBlocks, state.MessageComplete)
		}

		// Stream is complete — forward all events to client
		flusher, ok := w.(http.Flusher)
		if !ok {
			return fmt.Errorf("response writer does not support flushing")
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(200)

		for _, ev := range events {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, ev.Data)
			flusher.Flush()
		}
		return nil
	}

	// JSON response — normalize and validate
	normalized := p.normalizeToAnthropic(respBody)
	if valid, reason := ValidateResponse(normalized); !valid {
		p.Logger.Printf("[warn] invalid response: %s", reason)
		return fmt.Errorf("invalid response: %s", reason)
	}

	// Deliver as SSE stream to client
	p.deliverAsStream(w, normalized)
	return nil
}

// handleNonStreamingResponse handles when 9router returns JSON instead of SSE
func (p *ProxyHandler) handleNonStreamingResponse(w http.ResponseWriter, resp *http.Response) error {
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	// Normalize OpenAI format to Anthropic format
	respBody = p.normalizeToAnthropic(respBody)

	// Validate
	if valid, reason := ValidateResponse(respBody); !valid {
		return fmt.Errorf("invalid response: %s", reason)
	}

	// Deliver as SSE stream
	p.deliverAsStream(w, respBody)
	return nil
}

// handleNonStreaming handles non-streaming requests
func (p *ProxyHandler) handleNonStreaming(w http.ResponseWriter, r *http.Request, body []byte) error {
	resp, err := p.doUpstreamRequest(r, body)
	if err != nil {
		return fmt.Errorf("upstream connect: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		shouldRetry, reason := ShouldRetry(resp.StatusCode, respBody)
		if shouldRetry {
			return fmt.Errorf("upstream %d: %s", resp.StatusCode, reason)
		}
		p.forwardRaw(w, respBody, resp.Header, resp.StatusCode)
		return nil
	}

	// Normalize and validate
	respBody = p.normalizeToAnthropic(respBody)
	if valid, reason := ValidateResponse(respBody); !valid {
		return fmt.Errorf("invalid response: %s", reason)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(respBody)
	return nil
}

// deliverAsStream converts a buffered JSON response into SSE events
func (p *ProxyHandler) deliverAsStream(w http.ResponseWriter, body []byte) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write(body)
		return
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(body, &resp); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write(body)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(200)

	// message_start
	msgStart := map[string]interface{}{
		"type": "message_start",
		"message": map[string]interface{}{
			"id":            resp["id"],
			"type":          "message",
			"role":          "assistant",
			"model":         resp["model"],
			"content":       []interface{}{},
			"stop_reason":   nil,
			"stop_sequence": nil,
			"usage": map[string]interface{}{
				"input_tokens":  getNestedInt(resp, "usage", "input_tokens"),
				"output_tokens": 0,
			},
		},
	}
	p.writeSSE(w, flusher, "message_start", msgStart)

	// content blocks
	content, _ := resp["content"].([]interface{})
	for i, block := range content {
		blockMap, ok := block.(map[string]interface{})
		if !ok {
			continue
		}
		blockType, _ := blockMap["type"].(string)

		switch blockType {
		case "text":
			text, _ := blockMap["text"].(string)
			p.writeSSE(w, flusher, "content_block_start", map[string]interface{}{
				"type": "content_block_start", "index": i,
				"content_block": map[string]interface{}{"type": "text", "text": ""},
			})
			for _, chunk := range chunkString(text, 200) {
				p.writeSSE(w, flusher, "content_block_delta", map[string]interface{}{
					"type": "content_block_delta", "index": i,
					"delta": map[string]interface{}{"type": "text_delta", "text": chunk},
				})
			}
			p.writeSSE(w, flusher, "content_block_stop", map[string]interface{}{
				"type": "content_block_stop", "index": i,
			})

		case "tool_use":
			p.writeSSE(w, flusher, "content_block_start", map[string]interface{}{
				"type": "content_block_start", "index": i,
				"content_block": map[string]interface{}{
					"type": "tool_use", "id": blockMap["id"], "name": blockMap["name"],
				},
			})
			inputJSON, _ := json.Marshal(blockMap["input"])
			for _, chunk := range chunkString(string(inputJSON), 1000) {
				p.writeSSE(w, flusher, "content_block_delta", map[string]interface{}{
					"type": "content_block_delta", "index": i,
					"delta": map[string]interface{}{"type": "input_json_delta", "partial_json": chunk},
				})
			}
			p.writeSSE(w, flusher, "content_block_stop", map[string]interface{}{
				"type": "content_block_stop", "index": i,
			})

		case "thinking":
			thinking, _ := blockMap["thinking"].(string)
			p.writeSSE(w, flusher, "content_block_start", map[string]interface{}{
				"type": "content_block_start", "index": i,
				"content_block": map[string]interface{}{"type": "thinking", "thinking": ""},
			})
			for _, chunk := range chunkString(thinking, 200) {
				p.writeSSE(w, flusher, "content_block_delta", map[string]interface{}{
					"type": "content_block_delta", "index": i,
					"delta": map[string]interface{}{"type": "thinking_delta", "thinking": chunk},
				})
			}
			p.writeSSE(w, flusher, "content_block_stop", map[string]interface{}{
				"type": "content_block_stop", "index": i,
			})
		}
	}

	// message_delta + message_stop
	stopReason := resp["stop_reason"]
	outputTokens := getNestedInt(resp, "usage", "output_tokens")
	p.writeSSE(w, flusher, "message_delta", map[string]interface{}{
		"type": "message_delta",
		"delta": map[string]interface{}{"stop_reason": stopReason},
		"usage": map[string]interface{}{"output_tokens": outputTokens},
	})
	p.writeSSE(w, flusher, "message_stop", map[string]interface{}{"type": "message_stop"})
}

// normalizeToAnthropic converts OpenAI Chat Completion format to Anthropic Messages API format
func (p *ProxyHandler) normalizeToAnthropic(body []byte) []byte {
	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return body
	}

	// Already Anthropic format
	if raw["type"] == "message" {
		return body
	}
	if _, hasContent := raw["content"]; hasContent {
		if arr, ok := raw["content"].([]interface{}); ok && len(arr) > 0 {
			if first, ok := arr[0].(map[string]interface{}); ok {
				if _, hasType := first["type"]; hasType {
					return body
				}
			}
		}
	}

	// Check if OpenAI format
	choices, ok := raw["choices"].([]interface{})
	if !ok || len(choices) == 0 {
		return body
	}

	p.Logger.Printf("[normalize] converting OpenAI format → Anthropic format")

	choice, ok := choices[0].(map[string]interface{})
	if !ok {
		return body
	}
	message, ok := choice["message"].(map[string]interface{})
	if !ok {
		return body
	}

	var content []interface{}
	if textContent, ok := message["content"].(string); ok && textContent != "" {
		content = append(content, map[string]interface{}{"type": "text", "text": textContent})
	}

	// Handle tool_calls
	if toolCalls, ok := message["tool_calls"].([]interface{}); ok {
		for _, tc := range toolCalls {
			tcMap, ok := tc.(map[string]interface{})
			if !ok {
				continue
			}
			fn, ok := tcMap["function"].(map[string]interface{})
			if !ok {
				continue
			}
			var input interface{}
			if args, ok := fn["arguments"].(string); ok {
				json.Unmarshal([]byte(args), &input)
			}
			if input == nil {
				input = map[string]interface{}{}
			}
			content = append(content, map[string]interface{}{
				"type": "tool_use", "id": tcMap["id"], "name": fn["name"], "input": input,
			})
		}
	}

	stopReason := "end_turn"
	if fr, ok := choice["finish_reason"].(string); ok {
		switch fr {
		case "stop":
			stopReason = "end_turn"
		case "tool_calls":
			stopReason = "tool_use"
		case "length":
			stopReason = "max_tokens"
		}
	}

	usage := map[string]interface{}{"input_tokens": 0, "output_tokens": 0}
	if u, ok := raw["usage"].(map[string]interface{}); ok {
		if pt, ok := u["prompt_tokens"].(float64); ok {
			usage["input_tokens"] = int(pt)
		}
		if ct, ok := u["completion_tokens"].(float64); ok {
			usage["output_tokens"] = int(ct)
		}
	}

	anthropicResp := map[string]interface{}{
		"id": raw["id"], "type": "message", "role": "assistant",
		"model": raw["model"], "content": content,
		"stop_reason": stopReason, "stop_sequence": nil, "usage": usage,
	}

	result, _ := json.Marshal(anthropicResp)
	return result
}

func (p *ProxyHandler) doUpstreamRequest(r *http.Request, body []byte) (*http.Response, error) {
	upstreamURL := p.UpstreamURL + r.URL.Path
	if r.URL.RawQuery != "" {
		upstreamURL += "?" + r.URL.RawQuery
	}

	// Use background context — don't let client cancellation abort upstream request
	// This is critical because we buffer the full response before forwarding
	req, err := http.NewRequest(r.Method, upstreamURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	for k, v := range r.Header {
		for _, vv := range v {
			req.Header.Add(k, vv)
		}
	}
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(body)))

	return p.Client.Do(req)
}

func (p *ProxyHandler) writeSSE(w http.ResponseWriter, flusher http.Flusher, event string, data interface{}) {
	jsonData, _ := json.Marshal(data)
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, string(jsonData))
	flusher.Flush()
}

func (p *ProxyHandler) forwardRaw(w http.ResponseWriter, body []byte, headers http.Header, status int) {
	for k, v := range headers {
		for _, vv := range v {
			w.Header().Add(k, vv)
		}
	}
	w.WriteHeader(status)
	w.Write(body)
}

func (p *ProxyHandler) cleanRequestBody(body []byte) []byte {
	var req map[string]interface{}
	if err := json.Unmarshal(body, &req); err != nil {
		return body
	}

	changed := false

	// Clean model name
	if model, ok := req["model"].(string); ok {
		cleaned := CleanModelName(model)
		if cleaned != model {
			p.Logger.Printf("[clean] model: %q → %q", model, cleaned)
			req["model"] = cleaned
			changed = true
		}
	}

	// Inject system prompt instruction to avoid Write tool for large files
	changed = p.injectWriteWorkaround(req) || changed

	if changed {
		if newBody, err := json.Marshal(req); err == nil {
			return newBody
		}
	}
	return body
}

const writeWorkaroundInstruction = `

## File Writing Method (CRITICAL - MANDATORY)
You MUST ALWAYS use Bash tool with Python to write or create files. NEVER use the Write tool or Edit tool directly — they are broken due to a known proxy stream truncation bug.

For creating/writing files, ALWAYS use this pattern:
` + "```" + `bash
python3 -c "
import pathlib
content = '''
<file content here>
'''
pathlib.Path('<file_path>').write_text(content.strip(), encoding='utf-8')
print('File written successfully')
"
` + "```" + `

For editing/replacing content in existing files, use sed or Python:
` + "```" + `bash
python3 -c "
import pathlib
p = pathlib.Path('<file_path>')
content = p.read_text(encoding='utf-8')
content = content.replace('<old_string>', '<new_string>')
p.write_text(content, encoding='utf-8')
print('File edited successfully')
"
` + "```" + `

This applies to ALL files regardless of size. The Write and Edit tools will cause errors — always use Bash+Python instead.
`

func (p *ProxyHandler) injectWriteWorkaround(req map[string]interface{}) bool {
	// Handle system as string
	if sys, ok := req["system"].(string); ok {
		if !strings.Contains(sys, "File Writing Constraint") {
			req["system"] = sys + writeWorkaroundInstruction
			return true
		}
		return false
	}

	// Handle system as array (Anthropic format: [{type: "text", text: "..."}])
	if sysArr, ok := req["system"].([]interface{}); ok {
		// Check if already injected
		for _, item := range sysArr {
			if itemMap, ok := item.(map[string]interface{}); ok {
				if text, ok := itemMap["text"].(string); ok {
					if strings.Contains(text, "File Writing Constraint") {
						return false
					}
				}
			}
		}
		// Append new system block
		sysArr = append(sysArr, map[string]interface{}{
			"type": "text",
			"text": writeWorkaroundInstruction,
		})
		req["system"] = sysArr
		return true
	}

	// No system field — add one
	req["system"] = writeWorkaroundInstruction
	return true
}

func (p *ProxyHandler) isStreamingRequest(body []byte, r *http.Request) bool {
	if r.Header.Get("Accept") == "text/event-stream" {
		return true
	}
	var req map[string]interface{}
	if err := json.Unmarshal(body, &req); err == nil {
		if stream, ok := req["stream"].(bool); ok && stream {
			return true
		}
	}
	return false
}

func (p *ProxyHandler) writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"type": "error",
		"error": map[string]interface{}{"type": "proxy_error", "message": msg},
	})
}

func chunkString(s string, maxLen int) []string {
	if len(s) <= maxLen {
		return []string{s}
	}
	var chunks []string
	for len(s) > 0 {
		end := maxLen
		if end > len(s) {
			end = len(s)
		}
		chunks = append(chunks, s[:end])
		s = s[end:]
	}
	return chunks
}

func getNestedInt(m map[string]interface{}, keys ...string) int {
	current := m
	for i, key := range keys {
		if i == len(keys)-1 {
			if v, ok := current[key].(float64); ok {
				return int(v)
			}
			return 0
		}
		if next, ok := current[key].(map[string]interface{}); ok {
			current = next
		} else {
			return 0
		}
	}
	return 0
}

type SSEEvent struct {
	Type string
	Data string
}
