package mskconnect

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
)

// maxRolePolicies bounds the inline plus attached policies RolePolicies
// reads. IAM's default quotas are 10 attached managed policies per role and
// an inline total of 10,240 characters; a role over this bound is refused
// rather than read in part.
const maxRolePolicies = 32

// maxPolicyDocument bounds one policy document (IAM's limit for a managed
// policy is 6,144 characters).
const maxPolicyDocument = 64 << 10

// PolicyDocument is one permissions policy of a role: an inline policy
// (Kind "inline", Name) or an attached managed policy at its default version
// (Kind "attached", Arn).
type PolicyDocument struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Arn      string `json:"arn,omitempty"`
	Document string `json:"-"`
}

// RolePolicies reads every permissions policy of a role: the inline ones
// (ListRolePolicies, GetRolePolicy) and the attached managed ones at their
// default version (ListAttachedRolePolicies, GetPolicy, GetPolicyVersion).
// It returns all of them or an error, never a partial list, so a caller
// that must find a grant fails closed when one cannot be read.
func (c *Client) RolePolicies(ctx context.Context, arn string) ([]PolicyDocument, error) {
	if err := CheckRoleARN(arn); err != nil {
		return nil, err
	}
	name := arn[strings.LastIndexByte(arn, '/')+1:]
	var out []PolicyDocument
	add := func(p PolicyDocument) error {
		if len(out) >= maxRolePolicies {
			return fmt.Errorf("%w: role %s has more than %d policies", ErrInvalid, name, maxRolePolicies)
		}
		out = append(out, p)
		return nil
	}

	var marker *string
	for page := 0; ; page++ {
		if page == maxPages {
			return nil, fmt.Errorf("%w: list role policies: more than %d pages", ErrAWS, maxPages)
		}
		res, err := call(ctx, c, "list role policies", func(ctx context.Context) (*iam.ListRolePoliciesOutput, error) {
			return c.iam.ListRolePolicies(ctx, &iam.ListRolePoliciesInput{RoleName: aws.String(name), Marker: marker})
		})
		if err != nil {
			return nil, err
		}
		for _, pn := range res.PolicyNames {
			got, err := call(ctx, c, "get role policy", func(ctx context.Context) (*iam.GetRolePolicyOutput, error) {
				return c.iam.GetRolePolicy(ctx, &iam.GetRolePolicyInput{RoleName: aws.String(name), PolicyName: aws.String(pn)})
			})
			if err != nil {
				return nil, err
			}
			doc, err := policyText(str(got.PolicyDocument))
			if err != nil {
				return nil, fmt.Errorf("%w: inline policy %s: %v", ErrAWS, pn, err)
			}
			if err := add(PolicyDocument{Kind: "inline", Name: pn, Document: doc}); err != nil {
				return nil, err
			}
		}
		if !res.IsTruncated || res.Marker == nil {
			break
		}
		marker = res.Marker
	}

	marker = nil
	for page := 0; ; page++ {
		if page == maxPages {
			return nil, fmt.Errorf("%w: list attached role policies: more than %d pages", ErrAWS, maxPages)
		}
		res, err := call(ctx, c, "list attached role policies", func(ctx context.Context) (*iam.ListAttachedRolePoliciesOutput, error) {
			return c.iam.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{RoleName: aws.String(name), Marker: marker})
		})
		if err != nil {
			return nil, err
		}
		for _, ap := range res.AttachedPolicies {
			parn := str(ap.PolicyArn)
			pol, err := call(ctx, c, "get policy", func(ctx context.Context) (*iam.GetPolicyOutput, error) {
				return c.iam.GetPolicy(ctx, &iam.GetPolicyInput{PolicyArn: aws.String(parn)})
			})
			if err != nil {
				return nil, err
			}
			if pol.Policy == nil || pol.Policy.DefaultVersionId == nil {
				return nil, fmt.Errorf("%w: get policy %s: no default version", ErrAWS, parn)
			}
			ver, err := call(ctx, c, "get policy version", func(ctx context.Context) (*iam.GetPolicyVersionOutput, error) {
				return c.iam.GetPolicyVersion(ctx, &iam.GetPolicyVersionInput{PolicyArn: aws.String(parn), VersionId: pol.Policy.DefaultVersionId})
			})
			if err != nil {
				return nil, err
			}
			if ver.PolicyVersion == nil {
				return nil, fmt.Errorf("%w: get policy version %s: no version", ErrAWS, parn)
			}
			doc, err := policyText(str(ver.PolicyVersion.Document))
			if err != nil {
				return nil, fmt.Errorf("%w: policy %s: %v", ErrAWS, parn, err)
			}
			if err := add(PolicyDocument{Kind: "attached", Name: str(ap.PolicyName), Arn: parn, Document: doc}); err != nil {
				return nil, err
			}
		}
		if !res.IsTruncated || res.Marker == nil {
			break
		}
		marker = res.Marker
	}
	return out, nil
}

// policyText URL-decodes a policy document as IAM returns it (RFC 3986) and
// bounds it.
func policyText(raw string) (string, error) {
	if len(raw) > 3*maxPolicyDocument {
		return "", fmt.Errorf("the document is over %d bytes", maxPolicyDocument)
	}
	doc, err := url.QueryUnescape(raw)
	if err != nil {
		doc = raw
	}
	if doc == "" {
		return "", fmt.Errorf("the document is empty")
	}
	return doc, nil
}

// EscalationActions are the concrete IAM and STS actions a service
// execution role must not be granted: each lets the role grant itself or
// another principal more, or step into another role. EscalationGrants also
// refuses every iam:Put*, Attach*, Create*, Update*, Pass*, Add* and Set*
// action by its verb, so this list need not be complete for them.
var EscalationActions = []string{
	"iam:PutRolePolicy", "iam:PutUserPolicy", "iam:PutGroupPolicy", "iam:PutRolePermissionsBoundary",
	"iam:AttachRolePolicy", "iam:AttachUserPolicy", "iam:AttachGroupPolicy",
	"iam:CreatePolicyVersion", "iam:SetDefaultPolicyVersion",
	"iam:CreateAccessKey", "iam:CreateUser", "iam:CreateRole", "iam:CreateLoginProfile", "iam:UpdateLoginProfile",
	"iam:UpdateAssumeRolePolicy", "iam:PassRole", "iam:AddUserToGroup",
	"sts:AssumeRole",
}

// escalationVerbs are the IAM action verbs EscalationGrants refuses on any
// resource.
var escalationVerbs = []string{"put", "attach", "create", "update", "pass", "add", "set"}

// EscalationGrants reads one policy document and returns, for each Allow
// statement that could grant an escalation, a short description: an action
// pattern that matches any of EscalationActions (iam:*, iam:Put*, *, ...),
// a literal iam action with one of escalationVerbs, any sts:AssumeRole*,
// or a NotAction (which allows everything it does not name). Resource and
// Condition are ignored and Deny statements are not credited: the read
// fails closed, so a policy that allows iam:* and narrows it with a Deny is
// still reported. A document that cannot be parsed is reported too.
func EscalationGrants(doc string) []string {
	var pol struct {
		Statement json.RawMessage `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(doc), &pol); err != nil {
		return []string{"the policy is not a JSON policy document"}
	}
	var stmts []map[string]json.RawMessage
	if err := json.Unmarshal(pol.Statement, &stmts); err != nil {
		var one map[string]json.RawMessage
		if err := json.Unmarshal(pol.Statement, &one); err != nil || one == nil {
			return []string{"the policy has no statements"}
		}
		stmts = []map[string]json.RawMessage{one}
	}
	var out []string
	for i, st := range stmts {
		var effect string
		_ = json.Unmarshal(st["Effect"], &effect)
		if effect != "Allow" {
			continue
		}
		if _, ok := st["NotAction"]; ok {
			out = append(out, fmt.Sprintf("statement %d allows a NotAction", i+1))
			continue
		}
		actions, ok := stringOrList(st["Action"])
		if !ok {
			out = append(out, fmt.Sprintf("statement %d has an unreadable action list", i+1))
			continue
		}
		for _, a := range actions {
			if escalates(a) {
				out = append(out, fmt.Sprintf("statement %d allows %s", i+1, clip(a, 64)))
			}
		}
	}
	return out
}

// escalates reports whether an IAM action pattern could grant an
// escalation.
func escalates(pattern string) bool {
	p := strings.ToLower(pattern)
	for _, a := range EscalationActions {
		if globMatch(p, strings.ToLower(a)) {
			return true
		}
	}
	if strings.HasPrefix(p, "sts:assumerole") {
		return true
	}
	if verb, ok := strings.CutPrefix(p, "iam:"); ok {
		for _, v := range escalationVerbs {
			if strings.HasPrefix(verb, v) {
				return true
			}
		}
	}
	return false
}

// globMatch matches an IAM action pattern (* any run, ? one character)
// against a lowercase action name.
func globMatch(pattern, name string) bool {
	if pattern == "" {
		return name == ""
	}
	switch pattern[0] {
	case '*':
		for i := 0; i <= len(name); i++ {
			if globMatch(pattern[1:], name[i:]) {
				return true
			}
		}
		return false
	case '?':
		return name != "" && globMatch(pattern[1:], name[1:])
	}
	return name != "" && pattern[0] == name[0] && globMatch(pattern[1:], name[1:])
}
