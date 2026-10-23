package host

import (
	"errors"
	"runtime"
	"strings"
	"testing"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// The path rule tests of row U8b: both shapes are checked on every OS, because
// the runner renders for the host it runs on and its tests exercise the other
// shape. The two shapes differ exactly where the artifacts differ: systemd
// parses the unit text (no space, no backslash), the SCM escapes each argument
// (a space and parentheses are fine).

func TestCheckUnitPath(t *testing.T) {
	ok := []string{
		"/srv/piperoom", "/srv/piperoom/kafka", "/srv/p-1.2/connect_plugins", "/",
	}
	for _, p := range ok {
		if err := CheckUnitPath(p); err != nil {
			t.Errorf("CheckUnitPath(%q) = %v, want nil", p, err)
		}
	}
	bad := []string{
		"", "p", "./p", "srv/p", "/p/../x", "/p/", "//p", "/p/a b", "/p/%h", "/p/$X", `/p/"x"`,
		"/p/a\\b", "/p/a;b", "/p/\x00", "/p/" + strings.Repeat("a", maxTemplatePath),
	}
	for _, p := range bad {
		if err := CheckUnitPath(p); err == nil {
			t.Errorf("CheckUnitPath(%q) accepted", p)
		} else if !errors.Is(err, ErrDetect) {
			t.Errorf("CheckUnitPath(%q) = %v, want ErrDetect", p, err)
		}
	}
}

func TestCheckServicePath(t *testing.T) {
	ok := []string{
		// POSIX (the shape the Windows definition's tests use)
		"/srv/piperoom", "/srv/piperoom/kafka", "/srv/p-1.2/connect_plugins", "/",
		// Windows drive-absolute, either separator, a drive root, a mixed form
		`C:\Piperoom\orders`, "C:/Piperoom/orders", `C:\`, "C:/", `C:\p\kafka\libs`,
		`c:\users\utkarsh\appdata\local\temp\x`, `C:\Piperoom\orders\a,b`, `C:\Piperoom\orders\~tmp`,
		// The shapes the shared rule refused before U8b's review: the default
		// JDK under Program Files and a profile directory with a space.
		`C:\Program Files\Eclipse Adoptium\jdk-21`,
		`C:\Program Files (x86)\Piperoom\orders`,
		`C:\Users\Utkarsh Kumar\AppData\Local\piperoom\orders`,
		"C:/Program Files/Java/jdk-21/bin/java.exe",
		`C:\Program Files\Piperoom (old)\orders`,
	}
	for _, p := range ok {
		if err := CheckServicePath(p); err != nil {
			t.Errorf("CheckServicePath(%q) = %v, want nil", p, err)
		}
	}
	bad := []string{
		"", "p", "./p", "srv/p", "/p/../x", "/p/", "//p", "/p/a b", "/p/%h", "/p/$X", `/p/"x"`,
		"/p/a\\b", "/p/a;b", "/p/a=b", "/p/\x00", "/p/" + strings.Repeat("a", maxTemplatePath),
		// Windows a service definition still refuses
		`C:Piperoom`, `C:`, `\\host\share`, `\\?\C:\p`, `\p`, `\`,
		`C:\p\..\x`, `C:\p\.\x`, `C:\p\`, `C:\p\\x`, `C:\p\/x`, `C:\p\a%20`,
		`C:\p\a"b`, `C:\p\*`, `C:\p\a?b`, `C:\p\a|b`, `C:\p\a;b`, `C:\p\a=b`, "C:\\p\\a\x07",
		`C:\p\a.`, `C:\p\NUL`, `C:\p\con.txt`, `C:\p\COM1`, "C:\\p\\" + strings.Repeat("a", maxTemplatePath),
	}
	for _, p := range bad {
		if err := CheckServicePath(p); err == nil {
			t.Errorf("CheckServicePath(%q) accepted", p)
		} else if !errors.Is(err, ErrDetect) {
			t.Errorf("CheckServicePath(%q) = %v, want ErrDetect", p, err)
		}
	}
}

func TestCheckServiceToken(t *testing.T) {
	for _, v := range []string{
		"run-service", "--project", "--java", "-cp", "-Xms256M", "connect",
		"org.apache.kafka.connect.cli.ConnectDistributed", "/opt/jdk-21", `C:\jdk-21`, "C:/jdk-21",
		"-Dkafka.logs.dir=/srv/p/logs",
		`-Dkafka.logs.dir=C:\Program Files\Piperoom\logs`,
		`-Dlog4j2.configurationFile=C:\Program Files\Kafka\config\connect-log4j2.yaml`,
		`C:\Program Files\Eclipse Adoptium\jdk-21\bin\java.exe`,
		`C:\Program Files\Piperoom\kafka\libs\*`, "/srv/p/kafka/libs/*",
		`C:\Program Files\Piperoom\connect-distributed.properties`,
	} {
		if err := CheckServiceToken(v); err != nil {
			t.Errorf("CheckServiceToken(%q) = %v", v, err)
		}
	}
	for _, v := range []string{
		"", "a b", "a\nb", "..", "../x", `C:\a%20b`, "$HOME", "%h", `C:\p\..\x`,
		"-Dkafka.logs.dir=", `C:\Program Files\`,
	} {
		if err := CheckServiceToken(v); err == nil {
			t.Errorf("CheckServiceToken(%q) accepted", v)
		}
	}
}

// TestCheckUnitToken: the systemd shape refuses what the service shape allows,
// so one plan cannot render a unit systemd would split. A drive-absolute path
// is the one shape a Windows build accepts here, for the cross-OS tests (no
// Windows host runs systemd).
func TestCheckUnitToken(t *testing.T) {
	for _, v := range []string{
		"java", "-Xmx256M", "-Dkafka.logs.dir=/srv/p/logs", "/opt/jdk-21", "JAVA_HOME=/opt/jdk",
		"/srv/p/kafka/libs/*",
	} {
		if err := checkUnitToken(v); err != nil {
			t.Errorf("checkUnitToken(%q) = %v", v, err)
		}
	}
	for _, v := range []string{
		"", "a b", "a\nb", `C:\a b`, "opt/jdk", "$HOME", "%h", "/srv/p/a b",
	} {
		if err := checkUnitToken(v); err == nil {
			t.Errorf("checkUnitToken(%q) accepted", v)
		}
	}
	if runtime.GOOS != "windows" {
		if err := checkUnitToken(`C:\jdk-21`); err == nil {
			t.Error("checkUnitToken accepted a Windows path on a non-Windows build")
		}
	}
}

func TestCheckDefinitionValue(t *testing.T) {
	for _, v := range []string{"-Dkafka.logs.dir=C:\\p\\logs", "org.apache.kafka.connect.cli.ConnectDistributed", "a", "-Dkafka.logs.dir=C:\\Program Files\\p\\logs"} {
		if err := checkDefinitionValue(v); err != nil {
			t.Errorf("checkDefinitionValue(%q) = %v", v, err)
		}
	}
	for _, v := range []string{"", "a\nb", "a\rb", "a\x00b", strings.Repeat("a", maxTemplatePath+1)} {
		if err := checkDefinitionValue(v); err == nil {
			t.Errorf("checkDefinitionValue(%q) accepted", v)
		}
	}
}

func TestNativeJoin(t *testing.T) {
	cases := []struct {
		elem []string
		want string
	}{
		{[]string{"/srv/p", "kafka", "libs"}, "/srv/p/kafka/libs"},
		{[]string{`C:\p`, "kafka", "libs"}, `C:\p\kafka\libs`},
		{[]string{"C:/p", "bin", "piperoom-setup.exe"}, "C:/p/bin/piperoom-setup.exe"},
		{[]string{`C:\`, "bin"}, `C:\bin`},
		{[]string{"/", "bin"}, "/bin"},
		{[]string{"/srv/p/", "/kafka/"}, "/srv/p/kafka"},
	}
	for _, c := range cases {
		if got := nativeJoin(c.elem...); got != c.want {
			t.Errorf("nativeJoin(%q) = %q, want %q", c.elem, got, c.want)
		}
	}
}

func TestJavaHomeOf(t *testing.T) {
	ok := map[string]string{
		"/opt/jdk-21/bin/java":                                  "/opt/jdk-21",
		"/opt/jdk-21/bin/java.exe":                              "/opt/jdk-21",
		`C:\jdk-21\bin\java.exe`:                                `C:\jdk-21`,
		"C:/jdk-21/bin/java.exe":                                "C:/jdk-21",
		"C:/Users/u/jdk/bin/java.exe":                           "C:/Users/u/jdk",
		"/usr/lib/jvm/java-21/bin/java":                         "/usr/lib/jvm/java-21",
		`C:\Program Files\Eclipse Adoptium\jdk-21\bin\java.exe`: `C:\Program Files\Eclipse Adoptium\jdk-21`,
	}
	for in, want := range ok {
		got, err := javaHomeOf(in)
		if err != nil || got != want {
			t.Errorf("javaHomeOf(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{
		"", "java", "/opt/jdk/bin/javaw", "/opt/jdk/shims/java", "/opt/jdk/bin/java/",
		"/bin/java", `C:bin\java.exe`,
	} {
		if _, err := javaHomeOf(in); err == nil {
			t.Errorf("javaHomeOf(%q) accepted", in)
		}
	}
}

// TestPropertiesWindowsPaths: the properties and plugin path with Windows
// paths — a drive-letter project directory, a value written with forward
// slashes (Java reads those on every OS; a backslash in a .properties value is
// an escape), a space in the directory, and the same refusals as the POSIX
// form. The values are pure, so this runs on any OS.
func TestPropertiesWindowsPaths(t *testing.T) {
	dir := `C:\Piperoom\orders`
	wp, err := prepareProps(ModeDistributed, distributedEntries(), dir, protocol.ClassLocal)
	if err != nil {
		t.Fatalf("prepareProps: %v", err)
	}
	if got := wp.entries["plugin.path"]; got != "C:/Piperoom/orders/connect-plugins" {
		t.Errorf("plugin.path %q, want C:/Piperoom/orders/connect-plugins", got)
	}
	// An absolute Windows path and a backslash form are accepted and
	// rewritten to the slash form, case-insensitively on the drive and the
	// project components.
	for _, p := range []string{
		`C:\Piperoom\orders\connect-plugins\debezium`,
		`c:/piperoom/ORDERS/connect-plugins/debezium`,
		`connect-plugins/debezium`,
	} {
		wp, err := prepareProps(ModeDistributed, with(distributedEntries(), "plugin.path", p), dir, protocol.ClassLocal)
		if err != nil {
			t.Errorf("plugin.path %q: %v", p, err)
			continue
		}
		if got := wp.entries["plugin.path"]; got != "C:/Piperoom/orders/connect-plugins/debezium" {
			t.Errorf("plugin.path %q became %q", p, got)
		}
	}
	// A project under Program Files: the worker reads a value with a space
	// literally, and the Windows service passes the file path as one argument.
	spaced := `C:\Program Files\Piperoom\orders`
	wp, err = prepareProps(ModeDistributed, distributedEntries(), spaced, protocol.ClassLocal)
	if err != nil {
		t.Fatalf("Program Files project: %v", err)
	}
	if got := wp.entries["plugin.path"]; got != "C:/Program Files/Piperoom/orders/connect-plugins" {
		t.Errorf("plugin.path %q, want the Program Files directory", got)
	}
	// A standalone offset file, the same shape.
	standalone := map[string]string{
		"bootstrap.servers": "127.0.0.1:29092",
		"key.converter":     "org.apache.kafka.connect.json.JsonConverter",
		"value.converter":   "org.apache.kafka.connect.json.JsonConverter",
	}
	wp, err = prepareProps(ModeStandalone, standalone, dir, protocol.ClassLocal)
	if err != nil {
		t.Fatalf("standalone: %v", err)
	}
	if got := wp.entries["offset.storage.file.filename"]; got != "C:/Piperoom/orders/connect.offsets" {
		t.Errorf("offset file %q", got)
	}
	// Refusals: outside the project, the project root, a drive-relative path,
	// a POSIX path on a drive host, and an escape.
	for _, p := range []string{
		`C:\Other\connect-plugins`, `D:\Piperoom\orders\connect-plugins`, `C:connect-plugins`,
		`C:\Piperoom\orders`, `C:\Piperoom\orders\..\other`, `/srv/connect-plugins`,
		`...\connect-plugins`, `C:\Piperoom\orders\connect-plugins\..\..\x`,
	} {
		if _, err := prepareProps(ModeDistributed, with(distributedEntries(), "plugin.path", p), dir, protocol.ClassLocal); err == nil {
			t.Errorf("plugin.path %q accepted", p)
		} else if !errors.Is(err, ErrProperties) && !errors.Is(err, ops.ErrPath) {
			t.Errorf("plugin.path %q: %v, want ErrProperties or ErrPath", p, err)
		}
	}
	// A POSIX project directory keeps the POSIX form.
	wp, err = prepareProps(ModeDistributed, distributedEntries(), "/srv/orders", protocol.ClassLocal)
	if err != nil {
		t.Fatalf("posix: %v", err)
	}
	if got := wp.entries["plugin.path"]; got != "/srv/orders/connect-plugins" {
		t.Errorf("POSIX plugin.path %q", got)
	}
}

// TestProjectPathShapes pins the two accepted input shapes of a path value.
func TestProjectPathShapes(t *testing.T) {
	cases := []struct {
		dir, p, abs, rel string
	}{
		{"/srv/p", "connect-plugins", "/srv/p/connect-plugins", "connect-plugins"},
		{"/srv/p", "/srv/p/connect-plugins/", "/srv/p/connect-plugins", "connect-plugins"},
		{"/srv/p", "./connect-plugins", "/srv/p/connect-plugins", "connect-plugins"},
		{`C:\p`, `C:\p\connect-plugins`, "C:/p/connect-plugins", "connect-plugins"},
		{`C:\p`, `c:\P\connect-plugins`, "C:/p/connect-plugins", "connect-plugins"},
		{`C:\p`, "C:/p/connect-plugins", "C:/p/connect-plugins", "connect-plugins"},
	}
	for _, c := range cases {
		abs, rel, err := projectPath(c.dir, c.p)
		if err != nil || abs != c.abs || rel != c.rel {
			t.Errorf("projectPath(%q, %q) = %q, %q, %v; want %q, %q", c.dir, c.p, abs, rel, err, c.abs, c.rel)
		}
	}
}
