package mskconnect

import (
	"fmt"
	"regexp"
	"strings"
)

// Patterns every input is checked against before it reaches a request. They
// are stricter than AWS's own (a Piperoom setup names its resources with a
// prefix and a pipeline id) and need no I/O, so the setup ops call them from
// Validate.
var (
	// nameRE is a connector, plugin or worker configuration name.
	nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
	// resourceARN is a kafkaconnect resource ARN.
	resourceARN = regexp.MustCompile(`^arn:aws[a-z-]{0,12}:kafkaconnect:[a-z]{2}(-[a-z]+)+-[0-9]:[0-9]{12}:(connector|custom-plugin|worker-configuration)/[A-Za-z0-9_.-]{1,128}/[A-Za-z0-9-]{1,64}$`)
	// clusterARN is an MSK cluster ARN (provisioned or serverless).
	clusterARN = regexp.MustCompile(`^arn:aws[a-z-]{0,12}:kafka:[a-z]{2}(-[a-z]+)+-[0-9]:[0-9]{12}:cluster/[A-Za-z0-9-]{1,64}/[A-Za-z0-9-]{1,64}$`)
	// roleARN is an IAM role ARN with an optional path.
	roleARN = regexp.MustCompile(`^arn:aws[a-z-]{0,12}:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_-]{1,64}/){0,4}[A-Za-z0-9+=,.@_-]{1,64}$`)
	// bucketARN is an S3 bucket ARN.
	bucketARN  = regexp.MustCompile(`^arn:aws[a-z-]{0,12}:s3:::[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	// objectKey is a plugin archive key: path segments of safe characters,
	// ending in .zip or .jar.
	objectKey     = regexp.MustCompile(`^([A-Za-z0-9_.-]{1,100}/){0,8}[A-Za-z0-9_.-]{1,128}\.(zip|jar)$`)
	objectVersion = regexp.MustCompile(`^[A-Za-z0-9._-]{1,256}$`)
	subnetID      = regexp.MustCompile(`^subnet-[0-9a-f]{8,17}$`)
	groupID       = regexp.MustCompile(`^sg-[0-9a-f]{8,17}$`)
	// logGroup is a CloudWatch log group name.
	logGroup = regexp.MustCompile(`^[A-Za-z0-9_/.#-]{1,512}$`)
	// connectVersion is a Kafka Connect version string ("2.7.1", "3.7.x").
	connectVersion = regexp.MustCompile(`^[0-9]+\.[0-9]+(\.[0-9x]+)?$`)
	// currentVersion is a connector's CurrentVersion token.
	currentVersion = regexp.MustCompile(`^[A-Za-z0-9]{1,64}$`)
	// tagKey and tagValue bound the tags a setup puts on what it creates.
	tagKey   = regexp.MustCompile(`^[A-Za-z0-9_.:/=+@-]{1,128}$`)
	tagValue = regexp.MustCompile(`^[A-Za-z0-9_.:/=+@ -]{0,256}$`)
)

func check(re *regexp.Regexp, what, v string) error {
	if !re.MatchString(v) {
		return fmt.Errorf("%w: %s %q does not match %s", ErrInvalid, what, clip(v, 80), re.String())
	}
	return nil
}

// CheckName checks a connector, plugin or worker configuration name.
func CheckName(v string) error { return check(nameRE, "name", v) }

// CheckResourceARN checks a kafkaconnect connector, custom plugin or worker
// configuration ARN.
func CheckResourceARN(v string) error { return check(resourceARN, "MSK Connect ARN", v) }

// CheckClusterARN checks an MSK cluster ARN.
func CheckClusterARN(v string) error { return check(clusterARN, "MSK cluster ARN", v) }

// CheckRoleARN checks an IAM role ARN.
func CheckRoleARN(v string) error { return check(roleARN, "IAM role ARN", v) }

// CheckBucketARN checks an S3 bucket ARN.
func CheckBucketARN(v string) error { return check(bucketARN, "S3 bucket ARN", v) }

// CheckBucket checks an S3 bucket name.
func CheckBucket(v string) error { return check(bucketName, "S3 bucket", v) }

// CheckObjectKey checks a plugin archive key (a .zip or .jar).
func CheckObjectKey(v string) error {
	for _, seg := range strings.Split(v, "/") {
		if seg == "." || seg == ".." {
			return fmt.Errorf("%w: S3 object key %q has a dot segment", ErrInvalid, clip(v, 80))
		}
	}
	return check(objectKey, "S3 object key", v)
}

// CheckObjectVersion checks an S3 object version id; empty is allowed.
func CheckObjectVersion(v string) error {
	if v == "" {
		return nil
	}
	return check(objectVersion, "S3 object version", v)
}

// CheckSubnet checks a subnet id.
func CheckSubnet(v string) error { return check(subnetID, "subnet id", v) }

// CheckSecurityGroup checks a security group id.
func CheckSecurityGroup(v string) error { return check(groupID, "security group id", v) }

// CheckLogGroup checks a CloudWatch log group name.
func CheckLogGroup(v string) error { return check(logGroup, "log group", v) }

// CheckConnectVersion checks a Kafka Connect version string.
func CheckConnectVersion(v string) error { return check(connectVersion, "Kafka Connect version", v) }

// CheckTags checks the tags a create call carries.
func CheckTags(tags map[string]string) error {
	if len(tags) > 16 {
		return fmt.Errorf("%w: %d tags, at most 16", ErrInvalid, len(tags))
	}
	for k, v := range tags {
		if err := check(tagKey, "tag key", k); err != nil {
			return err
		}
		if err := check(tagValue, "tag value", v); err != nil {
			return err
		}
	}
	return nil
}

// AccountOfARN returns the account id field of an ARN, "" when there is
// none.
func AccountOfARN(arn string) string {
	parts := splitARN(arn)
	if len(parts) < 5 {
		return ""
	}
	return parts[4]
}

// RegionOfARN returns the region field of an ARN.
func RegionOfARN(arn string) string {
	parts := splitARN(arn)
	if len(parts) < 4 {
		return ""
	}
	return parts[3]
}

func splitARN(arn string) []string {
	out := make([]string, 0, 6)
	start := 0
	for i := 0; i < len(arn) && len(out) < 5; i++ {
		if arn[i] == ':' {
			out = append(out, arn[start:i])
			start = i + 1
		}
	}
	return append(out, arn[start:])
}
