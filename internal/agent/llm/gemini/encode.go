package gemini

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"piperoom/internal/agent/llm"
)

// wireRequest is a generateContent request body.
type wireRequest struct {
	Contents          []wireContent   `json:"contents"`
	SystemInstruction *wireContent    `json:"systemInstruction,omitempty"`
	Tools             []wireTool      `json:"tools,omitempty"`
	ToolConfig        *wireToolConfig `json:"toolConfig,omitempty"`
	GenerationConfig  *wireGenConfig  `json:"generationConfig,omitempty"`
}

type wireContent struct {
	Role  string            `json:"role,omitempty"`
	Parts []json.RawMessage `json:"parts"`
}

type wireTool struct {
	FunctionDeclarations []wireFunction `json:"functionDeclarations"`
}

type wireFunction struct {
	Name                 string         `json:"name"`
	Description          string         `json:"description,omitempty"`
	ParametersJSONSchema map[string]any `json:"parametersJsonSchema,omitempty"`
	Parameters           map[string]any `json:"parameters,omitempty"`
}

type wireToolConfig struct {
	FunctionCallingConfig struct {
		Mode string `json:"mode"`
	} `json:"functionCallingConfig"`
}

type wireGenConfig struct {
	MaxOutputTokens    int64               `json:"maxOutputTokens,omitempty"`
	ResponseMimeType   string              `json:"responseMimeType,omitempty"`
	ResponseJSONSchema map[string]any      `json:"responseJsonSchema,omitempty"`
	ResponseSchema     map[string]any      `json:"responseSchema,omitempty"`
	ThinkingConfig     *wireThinkingConfig `json:"thinkingConfig,omitempty"`
}

type wireThinkingConfig struct {
	ThinkingLevel  string `json:"thinkingLevel,omitempty"`
	ThinkingBudget *int64 `json:"thinkingBudget,omitempty"`
}

type wireFunctionCall struct {
	ID   string          `json:"id,omitempty"`
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
}

type wireFunctionResponse struct {
	ID       string `json:"id,omitempty"`
	Name     string `json:"name"`
	Response struct {
		Content string `json:"content"`
	} `json:"response"`
}

// malformedMarker is the key of the Native this adapter makes for a call it
// synthesised from a MALFORMED_FUNCTION_CALL stop. Gemini never sent such a
// part, so it never goes back as a functionCall (decodeResponse).
const malformedMarker = "piperoomMalformedFunctionCall"

var versionRe = regexp.MustCompile(`^gemini-(\d+)(?:\.(\d+))?`)

// version returns a gemini-<major>.<minor> model's version, ok false for
// another family (gemma, a tuned endpoint).
func version(model string) (major, minor int, ok bool) {
	m := versionRe.FindStringSubmatch(strings.TrimPrefix(model, "models/"))
	if m == nil {
		return 0, 0, false
	}
	major, _ = strconv.Atoi(m[1])
	if m[2] != "" {
		minor, _ = strconv.Atoi(m[2])
	}
	return major, minor, true
}

// modern reports Gemini 2.5 or later: parametersJsonSchema and
// responseJsonSchema, thinking, implicit caching.
func modern(model string) bool {
	major, minor, ok := version(model)
	return ok && (major > 2 || (major == 2 && minor >= 5))
}

// thinkingConfig maps the effort (design 3): Gemini 3 and later take a
// level, Gemini 2.5 a token budget; older models take none and drop it.
func thinkingConfig(model string, req llm.Request) *wireThinkingConfig {
	if !modern(model) || (req.Effort == "" && !req.Thinking) {
		return nil
	}
	if req.Effort == "" {
		return nil // the model's own dynamic thinking
	}
	major, _, _ := version(model)
	if major >= 3 {
		level := "high"
		if req.Effort == "low" {
			level = "low"
		}
		return &wireThinkingConfig{ThinkingLevel: level}
	}
	budget := map[string]int64{"low": 1024, "medium": 4096, "high": 8192, "xhigh": 16384, "max": 24576}[req.Effort]
	if budget == 0 {
		return nil
	}
	if req.MaxTokens > 0 && budget > req.MaxTokens/2 {
		budget = max(req.MaxTokens/2, 128)
	}
	return &wireThinkingConfig{ThinkingBudget: &budget}
}

// toolNameRe is Gemini's function name rule (design 7.1).
var toolNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.:-]{0,63}$`)

// CheckTools reports whether every tool can be sent to model: its name
// passes Gemini's rule, the count is under the cap, and its schema converts.
// The error names the tool; the start checks (V6) refuse start on it.
func CheckTools(model string, tools []llm.ToolDef) error {
	_, err := encodeTools(model, tools)
	return err
}

func encodeTools(model string, tools []llm.ToolDef) ([]wireTool, error) {
	if len(tools) == 0 {
		return nil, nil
	}
	if len(tools) > maxTools {
		return nil, fmt.Errorf("llm/gemini: %d tools, at most %d", len(tools), maxTools)
	}
	fns := make([]wireFunction, 0, len(tools))
	for _, t := range tools {
		if !toolNameRe.MatchString(t.Name) {
			return nil, fmt.Errorf("llm/gemini: tool %q: name breaks Gemini's rule (letter or _, then letters, digits, _ . : -, at most 64)", t.Name)
		}
		fn := wireFunction{Name: t.Name, Description: t.Description}
		if t.Schema != nil {
			if modern(model) {
				s, err := jsonSchema(t.Schema)
				if err != nil {
					return nil, fmt.Errorf("llm/gemini: tool %q: %w", t.Name, err)
				}
				fn.ParametersJSONSchema = s
			} else {
				s, err := openAPISchema(t.Schema, "")
				if err != nil {
					return nil, fmt.Errorf("llm/gemini: tool %q: %w", t.Name, err)
				}
				fn.Parameters = s
			}
		}
		fns = append(fns, fn)
	}
	return []wireTool{{FunctionDeclarations: fns}}, nil
}

// encodeRequest builds the body and the lower-bound token estimate of its
// content (design 6.2: content bytes / 4) for the truncation check.
func encodeRequest(model string, req llm.Request) ([]byte, int64, error) {
	tools, err := encodeTools(model, req.Tools)
	if err != nil {
		return nil, 0, err
	}
	var w wireRequest
	w.Tools = tools
	lowerBytes := len(req.System)
	for _, t := range req.Tools {
		b, _ := json.Marshal(t.Schema)
		lowerBytes += len(t.Name) + len(t.Description) + len(b)
	}
	if req.System != "" {
		p, err := encodeJSON(map[string]string{"text": req.System})
		if err != nil {
			return nil, 0, err
		}
		w.SystemInstruction = &wireContent{Parts: []json.RawMessage{p}}
	}
	if len(req.Tools) > 0 {
		mode := ""
		switch req.ToolChoice {
		case llm.ToolChoiceNone:
			mode = "NONE"
		case llm.ToolChoiceRequired:
			mode = "ANY"
		}
		if mode != "" {
			w.ToolConfig = &wireToolConfig{}
			w.ToolConfig.FunctionCallingConfig.Mode = mode
		}
	}
	gc := &wireGenConfig{MaxOutputTokens: req.MaxTokens, ThinkingConfig: thinkingConfig(model, req)}
	if req.OutputSchema != nil {
		gc.ResponseMimeType = "application/json"
		if modern(model) {
			s, err := jsonSchema(req.OutputSchema)
			if err != nil {
				return nil, 0, fmt.Errorf("llm/gemini: output schema: %w", err)
			}
			gc.ResponseJSONSchema = s
		} else {
			s, err := openAPISchema(req.OutputSchema, "")
			if err != nil {
				return nil, 0, fmt.Errorf("llm/gemini: output schema: %w", err)
			}
			gc.ResponseSchema = s
		}
	}
	if gc.MaxOutputTokens != 0 || gc.ResponseMimeType != "" || gc.ThinkingConfig != nil {
		w.GenerationConfig = gc
	}

	contents, n, err := encodeMessages(model, req.Messages)
	if err != nil {
		return nil, 0, err
	}
	lowerBytes += n
	w.Contents = contents
	body, err := encodeJSON(w)
	if err != nil {
		return nil, 0, fmt.Errorf("llm/gemini: encode request: %w", err)
	}
	return body, int64(lowerBytes / 4), nil
}

// callInfo is what a tool result needs from the call it answers.
type callInfo struct {
	name      string
	wireID    string // the id the call carried on the wire; "" = paired by position
	index     int    // position among the calls of its turn
	malformed bool
}

// encodeMessages translates the history (design 7.3) and returns the content
// bytes counted toward the lower-bound estimate.
func encodeMessages(model string, msgs []llm.Message) ([]wireContent, int, error) {
	calls := map[string]callInfo{}
	var out []wireContent
	n := 0
	for _, m := range msgs {
		switch m.Role {
		case llm.RoleAssistant:
			c := wireContent{Role: "model"}
			idx := 0
			for _, p := range m.Parts {
				raw, info, err := encodeModelPart(model, p)
				if err != nil {
					return nil, 0, err
				}
				if p.Kind == llm.PartToolCall && p.ToolCall != nil {
					info.index = idx
					idx++
					calls[p.ToolCall.ID] = info
					n += len(p.ToolCall.Name) + len(p.ToolCall.Input)
				}
				n += len(p.Text)
				if raw != nil {
					c.Parts = append(c.Parts, raw)
				}
			}
			if len(c.Parts) > 0 {
				out = append(out, c)
			}
		case llm.RoleUser:
			c := wireContent{Role: "user"}
			var results []llm.ToolResult
			var texts []string
			for _, p := range m.Parts {
				switch p.Kind {
				case llm.PartToolResult:
					if p.ToolResult != nil {
						results = append(results, *p.ToolResult)
					}
				case llm.PartText:
					texts = append(texts, p.Text)
				}
			}
			// Results go back in the order of the calls they answer: Gemini
			// pairs a functionResponse without an id with its call by position.
			sortByCall(results, calls)
			for _, r := range results {
				n += len(r.Content)
				info := calls[r.CallID]
				var raw []byte
				var err error
				if info.malformed {
					// The call went back as text; its result does too.
					raw, err = encodeJSON(map[string]string{"text": r.Content})
				} else {
					fr := wireFunctionResponse{ID: info.wireID, Name: r.Name}
					if fr.Name == "" {
						fr.Name = info.name
					}
					if fr.Name == "" {
						return nil, 0, fmt.Errorf("llm/gemini: tool result %q answers no known call and has no name", r.CallID)
					}
					fr.Response.Content = r.Content
					raw, err = encodeJSON(map[string]any{"functionResponse": fr})
				}
				if err != nil {
					return nil, 0, fmt.Errorf("llm/gemini: encode tool result: %w", err)
				}
				c.Parts = append(c.Parts, raw)
			}
			for _, t := range texts {
				n += len(t)
				raw, err := encodeJSON(map[string]string{"text": t})
				if err != nil {
					return nil, 0, fmt.Errorf("llm/gemini: encode text: %w", err)
				}
				c.Parts = append(c.Parts, raw)
			}
			if len(c.Parts) > 0 {
				out = append(out, c)
			}
		default:
			return nil, 0, fmt.Errorf("llm/gemini: unknown role %q", m.Role)
		}
	}
	return out, n, nil
}

// sortByCall orders results by the position of their call (stable; a result
// whose call is unknown keeps its place after the known ones).
func sortByCall(rs []llm.ToolResult, calls map[string]callInfo) {
	pos := func(r llm.ToolResult) int {
		if c, ok := calls[r.CallID]; ok {
			return c.index
		}
		return 1 << 30
	}
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && pos(rs[j]) < pos(rs[j-1]); j-- {
			rs[j], rs[j-1] = rs[j-1], rs[j]
		}
	}
}

// encodeModelPart encodes one assistant part. A part this adapter made for
// the same model goes back byte for byte (its thought signature included);
// any other part is rebuilt from the neutral fields, and a foreign reasoning
// part is dropped.
func encodeModelPart(model string, p llm.Part) (json.RawMessage, callInfo, error) {
	own := p.Native != nil && p.Native.Provider == llm.ProviderGemini && len(p.Native.Raw) > 0
	if own && isMalformed(p.Native.Raw) {
		name := ""
		if p.ToolCall != nil {
			name = p.ToolCall.Name
		}
		raw, err := encodeJSON(map[string]string{"text": "(malformed function call " + name + ")"})
		return raw, callInfo{name: name, malformed: true}, err
	}
	sameModel := own && strings.TrimPrefix(p.Native.Model, "models/") == model
	switch p.Kind {
	case llm.PartText:
		if sameModel {
			return p.Native.Raw, callInfo{}, nil
		}
		if p.Text == "" {
			return nil, callInfo{}, nil
		}
		raw, err := encodeJSON(map[string]string{"text": p.Text})
		return raw, callInfo{}, err
	case llm.PartToolCall:
		if p.ToolCall == nil {
			return nil, callInfo{}, errors.New("llm/gemini: a tool_call part without its call")
		}
		if sameModel {
			var w struct {
				FunctionCall wireFunctionCall `json:"functionCall"`
			}
			_ = json.Unmarshal(p.Native.Raw, &w)
			return p.Native.Raw, callInfo{name: p.ToolCall.Name, wireID: w.FunctionCall.ID}, nil
		}
		args := json.RawMessage(p.ToolCall.Input)
		var obj map[string]any
		if json.Unmarshal(args, &obj) != nil || obj == nil {
			args = json.RawMessage(`{}`)
		}
		raw, err := encodeJSON(map[string]any{"functionCall": wireFunctionCall{Name: p.ToolCall.Name, Args: args}})
		return raw, callInfo{name: p.ToolCall.Name}, err
	case llm.PartReasoning:
		if sameModel {
			return p.Native.Raw, callInfo{}, nil
		}
		return nil, callInfo{}, nil
	}
	return nil, callInfo{}, fmt.Errorf("llm/gemini: assistant part of kind %q", p.Kind)
}

func isMalformed(raw json.RawMessage) bool {
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return false
	}
	_, ok := m[malformedMarker]
	return ok
}
