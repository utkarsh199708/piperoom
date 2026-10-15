package kafkatools

import "strings"

// schemaPair is one from→to schema pair of the diff corpus.
type schemaPair struct{ name, from, to string }

func pRec(fields ...string) string {
	return `{"type":"record","name":"R","fields":[` + strings.Join(fields, ",") + `]}`
}
func pRecNS(ns string, fields ...string) string {
	return `{"type":"record","name":"R","namespace":"` + ns + `","fields":[` + strings.Join(fields, ",") + `]}`
}
func pF(name, typ string) string { return `{"name":"` + name + `","type":` + typ + `}` }
func pArr(items string) string   { return `{"type":"array","items":` + items + `}` }
func pMap(vals string) string    { return `{"type":"map","values":` + vals + `}` }
func pEnum(name, ns string, syms ...string) string {
	n := `"name":"` + name + `"`
	if ns != "" {
		n += `,"namespace":"` + ns + `"`
	}
	return `{"type":"enum",` + n + `,"symbols":["` + strings.Join(syms, `","`) + `"]}`
}

var (
	pFixedDec = `{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":10,"scale":2}`
	pFixedDur = `{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}`
)

// diffPairs is the corpus: every named-type position, moved or unchanged.
var diffPairs = []schemaPair{
	{"round1 reorder arrays enum def/ref",
		pRec(pF("a", pArr(pEnum("S", "", "A", "B"))), pF("b", pArr(`"S"`))),
		pRec(pF("b", pArr(`"S"`)), pF("a", pArr(`"S"`)))},
	{"round1 dropped defining field",
		pRec(pF("status", pEnum("S", "", "A", "B")), pF("hist", pArr(`"S"`))),
		pRec(pF("hist", pArr(pEnum("S", "", "A", "B"))))},
	{"map enum def moves to ref",
		pRec(pF("m", pMap(pEnum("S", "", "A", "B")))), pRec(pF("m", pMap(`"S"`)))},
	{"fixed decimal in array, fields swapped",
		pRec(pF("a", pArr(pFixedDec)), pF("b", pArr(`"D"`))),
		pRec(pF("b", pArr(`"D"`)), pF("a", pArr(`"D"`)))},
	{"fixed duration in map, fields swapped",
		pRec(pF("a", pMap(pFixedDur)), pF("b", pMap(`"Dur"`))),
		pRec(pF("b", pMap(`"Dur"`)), pF("a", pMap(`"Dur"`)))},
	{"same short enum name, two namespaces, swapped",
		pRec(pF("p", pEnum("S", "a", "A", "B")), pF("q", pEnum("S", "b", "X", "Y"))),
		pRec(pF("q", pEnum("S", "b", "X", "Y")), pF("p", pEnum("S", "a", "A", "B")))},
	{"same short enum name, field added before",
		pRec(pF("a", pEnum("S", "p", "A"))),
		pRec(pF("z", pEnum("S", "q", "X")), pF("a", pEnum("S", "p", "A")))},
	{"symbol added on the second same-name enum",
		pRec(pF("a", pEnum("S", "p", "A")), pF("b", pEnum("S", "q", "X"))),
		pRec(pF("a", pEnum("S", "p", "A")), pF("b", pEnum("S", "q", "X", "Y")))},
	{"namespace attribute vs dotted reference in array",
		pRec(pF("a", pArr(pEnum("S", "x", "A"))), pF("b", pArr(`"x.S"`))),
		pRec(pF("b", pArr(`"x.S"`)), pF("a", pArr(`"x.S"`)))},
	{"inherited namespace, short reference in array",
		`{"type":"record","name":"R","namespace":"x","fields":[` + pF("a", pArr(pEnum("S", "", "A"))) + `,` + pF("b", pArr(`"S"`)) + `]}`,
		`{"type":"record","name":"R","namespace":"x","fields":[` + pF("b", pArr(pEnum("S", "", "A"))) + `,` + pF("a", pArr(`"S"`)) + `]}`},
	{"unchanged: every named-type position",
		pRec(pF("e", pEnum("E", "", "A")), pF("r", `["null",{"type":"record","name":"N","fields":[{"name":"x","type":"int"}]}]`),
			pF("f", pFixedDec), pF("ae", pArr(`"E"`)), pF("mr", pMap(`"N"`)), pF("u", `["null","E"]`)),
		pRec(pF("e", pEnum("E", "", "A")), pF("r", `["null",{"type":"record","name":"N","fields":[{"name":"x","type":"int"}]}]`),
			pF("f", pFixedDec), pF("ae", pArr(`"E"`)), pF("mr", pMap(`"N"`)), pF("u", `["null","E"]`))},
	{"unchanged: Geometry reused through a union reference",
		pRec(pF("g", `["null",{"type":"record","name":"Geometry","fields":[{"name":"wkb","type":"bytes"}]}]`), pF("h", `["null","Geometry"]`)),
		pRec(pF("g", `["null",{"type":"record","name":"Geometry","fields":[{"name":"wkb","type":"bytes"}]}]`), pF("h", `["null","Geometry"]`))},
	{"enum symbol removed", pRec(pF("f", pEnum("E", "", "A", "B"))), pRec(pF("f", pEnum("E", "", "A")))},
	{"array item type changed", pRec(pF("f", pArr(`"int"`))), pRec(pF("f", pArr(`"long"`)))},
	{"nested record in array changed",
		pRec(pF("f", pArr(`{"type":"record","name":"N","fields":[{"name":"x","type":"int"}]}`))),
		pRec(pF("f", pArr(`{"type":"record","name":"N","fields":[{"name":"x","type":"long"}]}`)))},
	{"external ref in array, short vs full, enclosing namespace",
		pRecNS("com.x", pF("a", pArr(`"Addr"`))), pRecNS("com.x", pF("a", pArr(`"com.x.Addr"`)))},
	{"external ref in array, short vs full, no namespace",
		pRec(pF("a", pArr(`"Addr"`))), pRec(pF("a", pArr(`"com.x.Addr"`)))},
	{"external ref in map, short vs full, enclosing namespace",
		pRecNS("com.x", pF("m", pMap(`"Addr"`))), pRecNS("com.x", pF("m", pMap(`"com.x.Addr"`)))},
	{"external ref in map, short vs full, no namespace",
		pRec(pF("m", pMap(`"Addr"`))), pRec(pF("m", pMap(`"com.x.Addr"`)))},
	{"external ref in union in array, short vs full, enclosing namespace",
		pRecNS("com.x", pF("a", pArr(`["null","Addr"]`))), pRecNS("com.x", pF("a", pArr(`["null","com.x.Addr"]`)))},
	{"external ref in union in array, short vs full, no namespace",
		pRec(pF("a", pArr(`["null","Addr"]`))), pRec(pF("a", pArr(`["null","com.x.Addr"]`)))},
	{"type defined in v1 only, external reference in v2",
		pRecNS("com.x", pF("a", pArr(`{"type":"record","name":"Addr","fields":[{"name":"z","type":"int"}]}`))),
		pRecNS("com.x", pF("a", pArr(`"Addr"`)))},
	{"wrapped full ref vs bare short in array",
		pRecNS("com.x", pF("a", pArr(`{"type":"com.x.Addr"}`))), pRecNS("com.x", pF("a", pArr(`"Addr"`)))},
	{"wrapped full ref vs wrapped short in array",
		pRecNS("com.x", pF("a", pArr(`{"type":"com.x.Addr"}`))), pRecNS("com.x", pF("a", pArr(`{"type":"Addr"}`)))},
	{"wrapped full ref vs bare short in map",
		pRecNS("com.x", pF("m", pMap(`{"type":"com.x.Addr"}`))), pRecNS("com.x", pF("m", pMap(`"Addr"`)))},
	{"wrapped full ref in union in array vs bare short",
		pRecNS("com.x", pF("a", pArr(`["null",{"type":"com.x.Addr"}]`))), pRecNS("com.x", pF("a", pArr(`["null","Addr"]`)))},
	{"inline definition in v1, wrapped full ref in v2",
		pRecNS("com.x", pF("a", pArr(`{"type":"record","name":"Addr","fields":[{"name":"z","type":"int"}]}`))),
		pRecNS("com.x", pF("a", pArr(`{"type":"com.x.Addr"}`)))},
	{"wrapped full ref in array of array vs bare short",
		pRecNS("com.x", pF("a", pArr(pArr(`{"type":"com.x.Addr"}`)))), pRecNS("com.x", pF("a", pArr(pArr(`"Addr"`))))},
	{"wrapped full ref in map of union vs wrapped short",
		pRecNS("com.x", pF("m", pMap(`["null",{"type":"com.x.Addr"}]`))), pRecNS("com.x", pF("m", pMap(`["null",{"type":"Addr"}]`)))},
	{"doubly wrapped full ref vs bare short in array",
		pRecNS("com.x", pF("a", pArr(`{"type":{"type":"com.x.Addr"}}`))), pRecNS("com.x", pF("a", pArr(`"Addr"`)))},
	{"gap #493: namespace-only change of an element type in an array",
		pRec(pF("a", pArr(pEnum("S", "p", "A")))), pRec(pF("a", pArr(pEnum("S", "q", "A"))))},
	{"gap #493: fixed size change inside a map",
		pRec(pF("m", pMap(`{"type":"fixed","name":"D","size":8}`))), pRec(pF("m", pMap(`{"type":"fixed","name":"D","size":16}`)))},
	{"gap #493: fixed logicalType change inside an array",
		pRec(pF("a", pArr(`{"type":"fixed","name":"D","size":8}`))), pRec(pF("a", pArr(pFixedDec)))},
	{"gap #493: top-level external ref, short vs full",
		pRec(pF("a", `"Addr"`)), pRec(pF("a", `"com.x.Addr"`))},
	{"gap #493: plain-field definition vs reference",
		pRec(pF("a", pEnum("S", "", "A")), pF("b", `"S"`)),
		pRec(pF("b", pEnum("S", "", "A")), pF("a", `"S"`))},
}

// pairVerdict is what main answers and what the PR must answer for a pair.
// want may differ from main only where #425 names the case (why says how).
type pairVerdict struct{ main, want, why string }

const noChange = "no field changes"

var pairVerdicts = map[string]pairVerdict{
	"round1 reorder arrays enum def/ref":                                 {main: noChange, want: noChange},
	"round1 dropped defining field":                                      {main: "- R.status: enum:S", want: "- R.status: enum:S"},
	"map enum def moves to ref":                                          {main: noChange, want: noChange},
	"fixed decimal in array, fields swapped":                             {main: noChange, want: noChange},
	"fixed duration in map, fields swapped":                              {main: noChange, want: noChange},
	"same short enum name, two namespaces, swapped":                      {main: noChange, want: noChange},
	"same short enum name, field added before":                           {main: "+ R.z: enum:S", want: "+ R.z: enum:S"},
	"symbol added on the second same-name enum":                          {main: noChange, want: "enum q.S: symbols → +Y", why: "#425: enum symbols are compared"},
	"namespace attribute vs dotted reference in array":                   {main: noChange, want: noChange},
	"inherited namespace, short reference in array":                      {main: noChange, want: noChange},
	"unchanged: every named-type position":                               {main: noChange, want: noChange},
	"unchanged: Geometry reused through a union reference":               {main: noChange, want: noChange},
	"enum symbol removed":                                                {main: noChange, want: "enum E: symbols → -B", why: "#425: enum symbols are compared"},
	"array item type changed":                                            {main: noChange, want: "R.f: array<int> → array<long>", why: "#425: array items are compared"},
	"nested record in array changed":                                     {main: "N.x: int → long", want: "N.x: int → long"},
	"external ref in array, short vs full, enclosing namespace":          {main: noChange, want: noChange},
	"external ref in array, short vs full, no namespace":                 {main: noChange, want: noChange},
	"external ref in map, short vs full, enclosing namespace":            {main: noChange, want: noChange},
	"external ref in map, short vs full, no namespace":                   {main: noChange, want: noChange},
	"external ref in union in array, short vs full, enclosing namespace": {main: noChange, want: noChange},
	"external ref in union in array, short vs full, no namespace":        {main: noChange, want: noChange},
	"type defined in v1 only, external reference in v2":                  {main: "- Addr.z: int", want: "- Addr.z: int"},
	"wrapped full ref vs bare short in array":                            {main: noChange, want: noChange},
	"wrapped full ref vs wrapped short in array":                         {main: noChange, want: noChange},
	"wrapped full ref vs bare short in map":                              {main: noChange, want: noChange},
	"wrapped full ref in union in array vs bare short":                   {main: noChange, want: noChange},
	"inline definition in v1, wrapped full ref in v2":                    {main: "- Addr.z: int", want: "- Addr.z: int"},
	"wrapped full ref in array of array vs bare short":                   {main: noChange, want: noChange},
	"wrapped full ref in map of union vs wrapped short":                  {main: noChange, want: noChange},
	"doubly wrapped full ref vs bare short in array":                     {main: noChange, want: noChange},
	"gap #493: namespace-only change of an element type in an array":     {main: noChange, want: noChange},
	"gap #493: fixed size change inside a map":                           {main: noChange, want: noChange},
	"gap #493: fixed logicalType change inside an array":                 {main: noChange, want: noChange},
	// Known gap, same as main, pinned for follow-up #493.
	"gap #493: top-level external ref, short vs full": {main: "R.a: Addr → com.x.Addr", want: "R.a: Addr → com.x.Addr"},
	"gap #493: plain-field definition vs reference":   {main: "R.a: enum:S → S\nR.b: S → enum:S", want: "R.a: enum:S → S\nR.b: S → enum:S"},
}
