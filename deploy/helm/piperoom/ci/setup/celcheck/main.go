// Command celcheck compiles the setup pipeline release's admission policy
// (the D11 ValidatingAdmissionPolicy in a rendered file such as
// ../pipeline.golden.yaml) with cel-go and the Kubernetes strings library,
// and runs every case of cases.json through all of its validations: a case
// with allow true must pass every one, a case with allow false must fail at
// least one. The cases are objects the runner never renders but a
// compromised runner could send, and the shells and renders it does send.
//
//	cd deploy/helm/piperoom/ci/setup/celcheck && go run . ../pipeline.golden.yaml cases.json
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
	"gopkg.in/yaml.v3"
)

type tc struct {
	Name  string         `json:"name"`
	Allow bool           `json:"allow"`
	Obj   map[string]any `json:"obj"`
}

func main() {
	raw, _ := os.ReadFile(os.Args[1])
	var exprs []string
	for _, doc := range strings.Split(string(raw), "\n---\n") {
		var d map[string]any
		if yaml.Unmarshal([]byte(doc), &d) != nil || d["kind"] != "ValidatingAdmissionPolicy" {
			continue
		}
		for _, v := range d["spec"].(map[string]any)["validations"].([]any) {
			exprs = append(exprs, v.(map[string]any)["expression"].(string))
		}
	}
	if len(exprs) == 0 {
		fmt.Println("no ValidatingAdmissionPolicy validations in", os.Args[1])
		os.Exit(1)
	}
	env, err := cel.NewEnv(cel.Variable("object", cel.DynType), ext.Strings(ext.StringsVersion(2)))
	if err != nil {
		panic(err)
	}
	var progs []cel.Program
	for i, e := range exprs {
		ast, iss := env.Compile(e)
		if iss.Err() != nil {
			fmt.Printf("COMPILE %d: %v\n", i, iss.Err())
			os.Exit(1)
		}
		p, err := env.Program(ast)
		if err != nil {
			panic(err)
		}
		progs = append(progs, p)
	}
	fmt.Printf("%d expressions compile\n", len(progs))
	cb, _ := os.ReadFile(os.Args[2])
	var cases []tc
	if err := json.Unmarshal(cb, &cases); err != nil {
		panic(err)
	}
	bad := 0
	for _, c := range cases {
		c.Obj = ints(c.Obj).(map[string]any)
		allowed := true
		var why []string
		for i, p := range progs {
			out, _, err := p.Eval(map[string]any{"object": c.Obj})
			if err != nil {
				allowed = false
				why = append(why, fmt.Sprintf("#%d error %v", i, err))
				continue
			}
			if out.Value() != true {
				allowed = false
				why = append(why, fmt.Sprintf("#%d false", i))
			}
		}
		if allowed != c.Allow {
			bad++
			fmt.Printf("WRONG %s: allowed=%v want %v %v\n", c.Name, allowed, c.Allow, why)
		}
	}
	fmt.Printf("%d cases, %d wrong\n", len(cases), bad)
	if bad > 0 {
		os.Exit(1)
	}
}

// ints turns integral JSON numbers into int64, as the API server's
// unstructured objects carry them.
func ints(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			t[k] = ints(x)
		}
		return t
	case []any:
		for i, x := range t {
			t[i] = ints(x)
		}
		return t
	case float64:
		if t == float64(int64(t)) {
			return int64(t)
		}
	}
	return v
}
