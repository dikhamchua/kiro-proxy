package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

type StreamState struct {
	OpenBlocks      int
	MessageComplete bool
	CurrentToolUse  bool
	ToolInputBuf    strings.Builder
}

func NewStreamState() *StreamState {
	return &StreamState{}
}

func (s *StreamState) ProcessEvent(eventType, data string) {
	switch eventType {
	case "content_block_start":
		s.OpenBlocks++
		var block struct {
			ContentBlock struct {
				Type string `json:"type"`
			} `json:"content_block"`
		}
		if json.Unmarshal([]byte(data), &block) == nil {
			if block.ContentBlock.Type == "tool_use" {
				s.CurrentToolUse = true
				s.ToolInputBuf.Reset()
			}
		}
	case "content_block_delta":
		if s.CurrentToolUse {
			var delta struct {
				Delta struct {
					Type        string `json:"type"`
					PartialJSON string `json:"partial_json"`
				} `json:"delta"`
			}
			if json.Unmarshal([]byte(data), &delta) == nil {
				if delta.Delta.Type == "input_json_delta" {
					s.ToolInputBuf.WriteString(delta.Delta.PartialJSON)
				}
			}
		}
	case "content_block_stop":
		s.OpenBlocks--
		s.CurrentToolUse = false
	case "message_stop":
		s.MessageComplete = true
	}
}

func (s *StreamState) IsIncomplete() bool {
	return s.OpenBlocks > 0 || (!s.MessageComplete && s.OpenBlocks == 0)
}

func ValidateResponse(body []byte) (bool, string) {
	if len(body) == 0 {
		return false, "empty response body"
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(body, &resp); err != nil {
		// Log first 200 chars of body for debugging
		preview := string(body)
		if len(preview) > 200 {
			preview = preview[:200]
		}
		return false, fmt.Sprintf("invalid JSON response (preview: %s)", preview)
	}

	// Check if it's an error response from upstream
	if errObj, ok := resp["error"]; ok {
		if errMap, ok := errObj.(map[string]interface{}); ok {
			msg, _ := errMap["message"].(string)
			return false, "upstream error: " + msg
		}
	}

	// Validate tool_use blocks if content exists
	content, ok := resp["content"]
	if !ok {
		// No content field — might be a different format, pass through
		return true, ""
	}

	contentArr, ok := content.([]interface{})
	if !ok {
		return true, ""
	}

	for _, block := range contentArr {
		blockMap, ok := block.(map[string]interface{})
		if !ok {
			continue
		}
		if blockMap["type"] == "tool_use" {
			name, _ := blockMap["name"].(string)
			inputRaw, _ := json.Marshal(blockMap["input"])
			if valid, reason := ValidateToolUseBlock(name, inputRaw); !valid {
				return false, reason
			}
		}
	}

	return true, ""
}

func ValidateToolUseBlock(name string, input json.RawMessage) (bool, string) {
	if len(input) == 0 || string(input) == "null" {
		return false, "tool_use block has empty input"
	}

	var params map[string]interface{}
	if err := json.Unmarshal(input, &params); err != nil {
		return false, "tool_use input is not valid JSON"
	}

	switch name {
	case "Write":
		if _, ok := params["file_path"]; !ok {
			return false, "Write tool missing file_path"
		}
		if _, ok := params["content"]; !ok {
			return false, "Write tool missing content"
		}
	case "Edit":
		if _, ok := params["file_path"]; !ok {
			return false, "Edit tool missing file_path"
		}
		if _, ok := params["old_string"]; !ok {
			return false, "Edit tool missing old_string"
		}
		if _, ok := params["new_string"]; !ok {
			return false, "Edit tool missing new_string"
		}
	}

	return true, ""
}

var modelSuffixPattern = regexp.MustCompile(`\[\d+m\]`)

func CleanModelName(model string) string {
	cleaned := modelSuffixPattern.ReplaceAllString(model, "")
	cleaned = strings.TrimSuffix(cleaned, "-review")
	return cleaned
}
