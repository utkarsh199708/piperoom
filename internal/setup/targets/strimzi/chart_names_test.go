package strimzi

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"gopkg.in/yaml.v3"
)

// The chart (deploy/helm/piperoom, row U12c) pre-creates every object the
// driver reaches, and the runner's Role names them; a name the two spell
// differently is a 403 or a missing shell at the first kind run. This test
// reads the chart's golden renders and pins them to the driver's names.

const chartGolden = "../../../../deploy/helm/piperoom/ci/setup"

type k8sObject struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name        string            `yaml:"name"`
		Annotations map[string]string `yaml:"annotations"`
	} `yaml:"metadata"`
	Rules []struct {
		Resources     []string `yaml:"resources"`
		ResourceNames []string `yaml:"resourceNames"`
		Verbs         []string `yaml:"verbs"`
	} `yaml:"rules"`
}

func readGolden(t *testing.T, name string) []k8sObject {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(chartGolden, name))
	if err != nil {
		t.Fatalf("reading the chart's golden render: %v", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	var out []k8sObject
	for {
		var o k8sObject
		err := dec.Decode(&o)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if o.Kind != "" {
			out = append(out, o)
		}
	}
}

func TestChartShellsAreTheDriversNames(t *testing.T) {
	h := newHarness(t, "", nil)
	d := h.d
	objs := readGolden(t, "pipeline.golden.yaml")
	have := map[string]bool{}
	for _, o := range objs {
		have[o.Kind+"/"+o.Metadata.Name] = true
	}
	for _, want := range []string{
		"KafkaConnect/" + d.connectName(),
		"KafkaConnector/orders-src",
		"KafkaUser/" + d.connectName(),
		"KafkaUser/" + d.readUserName(),
		"KafkaTopic/" + d.canaryTopicName(),
		"Secret/" + d.dbSecretName(),
		"ConfigMap/" + d.loggingName(),
		"ConfigMap/" + d.metricsName(),
		"ValidatingAdmissionPolicyBinding/piperoom-pipeline-" + testPipeline,
	} {
		if !have[want] {
			t.Errorf("the pipeline release renders no %s", want)
		}
	}
}

func TestChartRoleNamesTheDriversObjects(t *testing.T) {
	h := newHarness(t, "", nil)
	d := h.d
	var role *k8sObject
	for _, o := range readGolden(t, "runner.golden.yaml") {
		if o.Kind == "Role" {
			role = &o
		}
	}
	if role == nil {
		t.Fatal("the runner release renders no Role")
	}
	names := map[string][]string{}
	for _, r := range role.Rules {
		for _, res := range r.Resources {
			names[res] = append(names[res], r.ResourceNames...)
		}
	}
	for res, want := range map[string][]string{
		"kafkas":        {testKafka},
		"kafkaconnects": {d.connectName()},
		"kafkausers":    {d.connectName(), d.readUserName()},
		"kafkatopics":   {d.canaryTopicName()},
		"secrets":       {d.dbSecretName(), d.readUserName()},
		"configmaps":    {d.loggingName(), d.metricsName()},
		"pods":          d.podNames(),
		"pods/log":      d.podNames(),
	} {
		for _, n := range want {
			if !slices.Contains(names[res], n) {
				t.Errorf("the Role does not name %s %s (it names %v)", res, n, names[res])
			}
		}
	}
}
