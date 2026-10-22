package ops

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// shellNames are programs that run a command string. No op may name one: an
// exec op builds an argv slice (design §2.5: there is no shell).
var shellNames = map[string]bool{
	"sh": true, "sh.exe": true, "bash": true, "bash.exe": true, "dash": true,
	"zsh": true, "ksh": true, "cmd": true, "cmd.exe": true, "powershell": true,
	"powershell.exe": true, "pwsh": true, "pwsh.exe": true, "busybox": true,
	"/bin/sh": true, "/bin/bash": true, "/bin/zsh": true, "/usr/bin/env": true,
}

// shellFlags are the argv elements that turn a program into a command-string
// runner.
var shellFlags = map[string]bool{"-c": true, "/c": true, "/C": true, "-Command": true, "-command": true}

// TestNoShellExecution is the AST test the row asks for (the shape of
// internal/wiring/readonly_ast_test.go): every os/exec call in the package is
// exec.CommandContext with a literal program that is not a shell, and every
// call to the package's runCommand names a literal program.
func TestNoShellExecution(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			// A direct call to the package helper must name a literal.
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "runCommand" {
				checkLiteralProgram(t, fset, call, 0)
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			switch {
			case pkg.Name == "syscall" && (sel.Sel.Name == "Exec" || sel.Sel.Name == "StartProcess"):
				t.Errorf("%s: syscall.%s bypasses the argv rule", fset.Position(call.Pos()), sel.Sel.Name)
			case pkg.Name == "exec" && sel.Sel.Name == "Command":
				t.Errorf("%s: exec.Command without a context", fset.Position(call.Pos()))
			case pkg.Name == "exec" && sel.Sel.Name == "CommandContext":
				checked++
				if len(call.Args) < 2 {
					t.Errorf("%s: CommandContext without a program", fset.Position(call.Pos()))
					return true
				}
				program := call.Args[1]
				if id, ok := program.(*ast.Ident); ok {
					// runCommand passes its own parameter; that is the one
					// indirection the package allows, and every call to it
					// names a literal.
					if id.Name != "name" || name != "exec.go" {
						t.Errorf("%s: program %q is not a literal", fset.Position(call.Pos()), id.Name)
					}
				} else {
					checkLiteralProgram(t, fset, call, 1)
				}
				for _, a := range call.Args[2:] {
					alit, ok := a.(*ast.BasicLit)
					if !ok || alit.Kind != token.STRING {
						continue
					}
					arg, _ := strconv.Unquote(alit.Value)
					if shellFlags[arg] {
						t.Errorf("%s: argv carries the shell flag %q", fset.Position(call.Pos()), arg)
					}
				}
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no exec.CommandContext call found: the check would pass vacuously")
	}
}

// checkLiteralProgram asserts call.Args[i] is a non-shell string literal.
func checkLiteralProgram(t *testing.T, fset *token.FileSet, call *ast.CallExpr, i int) {
	t.Helper()
	if len(call.Args) <= i {
		t.Errorf("%s: call has no program argument", fset.Position(call.Pos()))
		return
	}
	lit, ok := call.Args[i].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		t.Errorf("%s: program is not a string literal", fset.Position(call.Pos()))
		return
	}
	prog, _ := strconv.Unquote(lit.Value)
	if shellNames[strings.ToLower(prog)] {
		t.Errorf("%s: program %q is a shell", fset.Position(call.Pos()), prog)
	}
}
