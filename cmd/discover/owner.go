package main

import (
	"os/user"
	"strings"

	"piperoom/internal/discover"
)

// fallbackOwner is the user id when neither the OS nor $USER gives a usable one.
const fallbackOwner = "piperoom"

// currentUsername is the OS user name, for runner.osUser.
func currentUsername() (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", err
	}
	return u.Username, nil
}

// defaultOwner picks the user id that owns every layer when -owner is not
// given: the OS user name, then $USER, then "piperoom". A candidate that the
// server would refuse as a user id (a name with a space, a colon, a "u_"
// prefix) is skipped rather than written into a file that cannot start the
// server. On Windows the OS name is DOMAIN\name; only the name is kept.
func defaultOwner(osUser func() (string, error), getenv func(string) string) string {
	var candidates []string
	if name, err := osUser(); err == nil {
		candidates = append(candidates, name)
	}
	candidates = append(candidates, getenv("USER"))
	for _, c := range candidates {
		if i := strings.LastIndex(c, `\`); i >= 0 {
			c = c[i+1:]
		}
		if c = strings.TrimSpace(c); discover.ValidateOwner(c) == nil {
			return c
		}
	}
	return fallbackOwner
}
