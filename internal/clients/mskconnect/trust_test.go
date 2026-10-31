package mskconnect

import "testing"

// TestTrustProblem (review L10): a service execution role is trusted by
// kafkaconnect.amazonaws.com alone. strings.Contains on the document, the
// old check, passed every row below that names the service anywhere.
func TestTrustProblem(t *testing.T) {
	ok := []string{
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"kafkaconnect.amazonaws.com"},"Action":"sts:AssumeRole"}]}`,
		`{"Version":"2012-10-17","Statement":{"Effect":"Allow","Principal":{"Service":["kafkaconnect.amazonaws.com"]},"Action":["sts:AssumeRole"],
			"Condition":{"StringEquals":{"aws:SourceAccount":"000000000000"}}}}`,
		`{"Statement":[{"Effect":"Allow","Principal":{"Service":"kafkaconnect.amazonaws.com"},"Action":"sts:AssumeRole"},{"Effect":"Deny","Principal":"*","Action":"sts:AssumeRole"}]}`,
	}
	for _, doc := range ok {
		if p := TrustProblem(doc); p != "" {
			t.Errorf("%s: %s", doc, p)
		}
	}
	bad := map[string]string{
		"another account too":   `{"Statement":[{"Effect":"Allow","Principal":{"Service":"kafkaconnect.amazonaws.com","AWS":"arn:aws:iam::111111111111:root"},"Action":"sts:AssumeRole"}]}`,
		"a second statement":    `{"Statement":[{"Effect":"Allow","Principal":{"Service":"kafkaconnect.amazonaws.com"},"Action":"sts:AssumeRole"},{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}`,
		"a second service":      `{"Statement":[{"Effect":"Allow","Principal":{"Service":["kafkaconnect.amazonaws.com","ec2.amazonaws.com"]},"Action":"sts:AssumeRole"}]}`,
		"everyone":              `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"sts:AssumeRole","Condition":{"StringEquals":{"aws:PrincipalService":"kafkaconnect.amazonaws.com"}}}]}`,
		"web identity":          `{"Statement":[{"Effect":"Allow","Principal":{"Federated":"kafkaconnect.amazonaws.com"},"Action":"sts:AssumeRoleWithWebIdentity"}]}`,
		"NotPrincipal":          `{"Statement":[{"Effect":"Allow","NotPrincipal":{"Service":"ec2.amazonaws.com"},"Principal":{"Service":"kafkaconnect.amazonaws.com"},"Action":"sts:AssumeRole"}]}`,
		"a wildcard action":     `{"Statement":[{"Effect":"Allow","Principal":{"Service":"kafkaconnect.amazonaws.com"},"Action":"sts:*"}]}`,
		"no allow":              `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"sts:AssumeRole"}]}`,
		"a name in a condition": `{"Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::111111111111:root"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"x":"kafkaconnect.amazonaws.com"}}}]}`,
		"not JSON":              `kafkaconnect.amazonaws.com`,
	}
	for name, doc := range bad {
		if p := TrustProblem(doc); p == "" {
			t.Errorf("%s: no problem found in %s", name, doc)
		}
	}
}

// TestEscalationGrants (review round 3): a policy statement that could let
// the role grant itself more or assume another role is reported, whatever
// its resource; one that grants only the connector's actions is not.
func TestEscalationGrants(t *testing.T) {
	bad := map[string]string{
		"iam:*":                  `{"Statement":[{"Effect":"Allow","Action":"iam:*","Resource":"*"}]}`,
		"*":                      `{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		"PutRolePolicy on self":  `{"Statement":[{"Effect":"Allow","Action":"iam:PutRolePolicy","Resource":"arn:aws:iam::000000000000:role/piperoom-msk"}]}`,
		"AssumeRole on an admin": `{"Statement":{"Effect":"Allow","Action":["logs:PutLogEvents","sts:AssumeRole"],"Resource":"arn:aws:iam::000000000000:role/Admin"}}`,
		"a glob":                 `{"Statement":[{"Effect":"Allow","Action":"iam:Attach*","Resource":"*"}]}`,
		"a ? glob":               `{"Statement":[{"Effect":"Allow","Action":"iam:PassRol?","Resource":"*"}]}`,
		"lower case":             `{"Statement":[{"Effect":"Allow","Action":"IAM:createloginprofile","Resource":"*"}]}`,
		"a Create verb":          `{"Statement":[{"Effect":"Allow","Action":"iam:CreateInstanceProfile","Resource":"*"}]}`,
		"NotAction":              `{"Statement":[{"Effect":"Allow","NotAction":"s3:*","Resource":"*"}]}`,
		"sts:AssumeRoleWith*":    `{"Statement":[{"Effect":"Allow","Action":"sts:AssumeRoleWithWebIdentity","Resource":"*"}]}`,
		"a Deny is not credited": `{"Statement":[{"Effect":"Allow","Action":"iam:*","Resource":"*"},{"Effect":"Deny","Action":"iam:*","Resource":"*"}]}`,
		"not JSON":               `iam`,
	}
	for name, doc := range bad {
		if len(EscalationGrants(doc)) == 0 {
			t.Errorf("%s: no grant found in %s", name, doc)
		}
	}
	ok := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["kafka-cluster:Connect","kafka-cluster:DescribeCluster","logs:CreateLogStream","logs:PutLogEvents","s3:GetObject","iam:GetRole","iam:ListRoles"],"Resource":"*"},{"Effect":"Deny","Action":"iam:*","Resource":"*"}]}`
	if g := EscalationGrants(ok); len(g) != 0 {
		t.Errorf("a connector-only policy: %v", g)
	}
}
