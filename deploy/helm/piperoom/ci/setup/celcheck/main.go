// Command celcheck compiles the setup pipeline release's admission policy
// (the D11 ValidatingAdmissionPolicy in a rendered file such as
// ../pipeline.golden.yaml) with cel-go and the Kubernetes strings library,
// and runs every case of cases.json through all of its validations: a case
// with allow true must pass every one, a case with allow false must fail at
// least one. The cases are objects the runner never renders but a
// compromised runner could send, and the shells and renders it does send.
//
// Further case files after the first are read the same way: renders.json
// holds the objects the runner writes for the CI values (written by
// internal/setup/targets/strimzi policy_files_test.go), all allow cases.
//
//	cd deploy/helm/piperoom/ci/setup/celcheck && go run . ../pipeline.golden.yaml cases.json renders.json
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
	type variable struct{ name, expr string }
	var vars []variable
	for _, doc := range strings.Split(string(raw), "\n---\n") {
		var d map[string]any
		if yaml.Unmarshal([]byte(doc), &d) != nil || d["kind"] != "ValidatingAdmissionPolicy" {
			continue
		}
		spec := d["spec"].(map[string]any)
		for _, v := range spec["validations"].([]any) {
			exprs = append(exprs, v.(map[string]any)["expression"].(string))
		}
		if vs, ok := spec["variables"].([]any); ok {
			for _, v := range vs {
				m := v.(map[string]any)
				vars = append(vars, variable{m["name"].(string), m["expression"].(string)})
			}
		}
	}
	if len(exprs) == 0 {
		fmt.Println("no ValidatingAdmissionPolicy validations in", os.Args[1])
		os.Exit(1)
	}
	// spec.variables are evaluated in order, each seeing the ones before it
	// as variables.<name>, as the API server does; one that fails to
	// evaluate is left out, so a validation that reads it errors (denies).
	env, err := cel.NewEnv(cel.Variable("object", cel.DynType), cel.Variable("variables", cel.MapType(cel.StringType, cel.DynType)), ext.Strings(ext.StringsVersion(2)))
	if err != nil {
		panic(err)
	}
	compile := func(i int, e string) cel.Program {
		ast, iss := env.Compile(e)
		if iss.Err() != nil {
			fmt.Printf("COMPILE %d: %v\n", i, iss.Err())
			os.Exit(1)
		}
		p, err := env.Program(ast)
		if err != nil {
			panic(err)
		}
		return p
	}
	varProgs := make([]cel.Program, len(vars))
	for i, v := range vars {
		varProgs[i] = compile(-1-i, v.expr)
	}
	var progs []cel.Program
	for i, e := range exprs {
		progs = append(progs, compile(i, e))
	}
	evalVars := func(obj map[string]any) map[string]any {
		out := map[string]any{}
		for i, v := range vars {
			r, _, err := varProgs[i].Eval(map[string]any{"object": obj, "variables": out})
			if err == nil {
				out[v.name] = r
			}
		}
		return out
	}
	fmt.Printf("%d expressions compile\n", len(progs))
	var cases []tc
	for _, f := range os.Args[2:] {
		cb, err := os.ReadFile(f)
		if err != nil {
			panic(err)
		}
		var cs []tc
		if err := json.Unmarshal(cb, &cs); err != nil {
			panic(fmt.Sprintf("%s: %v", f, err))
		}
		cases = append(cases, cs...)
	}
	bad := 0
	for _, c := range cases {
		c.Obj = ints(c.Obj).(map[string]any)
		allowed := true
		var why []string
		vs := evalVars(c.Obj)
		for i, p := range progs {
			out, _, err := p.Eval(map[string]any{"object": c.Obj, "variables": vs})
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
