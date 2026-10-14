package loadtest

import (
	"errors"
	"testing"
)

func TestRateCheck(t *testing.T) {
	tests := []struct {
		name              string
		offered, achieved float64
		want              bool
	}{
		{"keeps up", 90, 90, true},
		{"at the floor", 100, 90, true},
		{"QA10: 67 of 90 accepted", 90, 67, false},
		{"nothing offered", 0, 0, false},
	}
	for _, tc := range tests {
		if got := rateCheck(tc.offered, tc.achieved); got.OK != tc.want {
			t.Errorf("%s: rateCheck(%v, %v).OK = %v, want %v (%s)", tc.name, tc.offered, tc.achieved, got.OK, tc.want, got.Detail)
		}
	}
}

func TestIsImagePullError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{errors.New("docker run: exit status 125: Unable to find image 'postgres:16' locally\ndocker: Error response from daemon: toomanyrequests: You have reached your pull rate limit."), true},
		{errors.New("docker: Error response from daemon: Head \"https://registry-1.docker.io/v2/library/postgres/manifests/16\": 429 Too Many Requests"), true},
		{errors.New("docker: Error response from daemon: pull access denied for postgres"), true},
		{errors.New("docker: Error response from daemon: Conflict. The container name is already in use"), false},
		{nil, false},
	}
	for _, tc := range tests {
		if got := isImagePullError(tc.err); got != tc.want {
			t.Errorf("isImagePullError(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
