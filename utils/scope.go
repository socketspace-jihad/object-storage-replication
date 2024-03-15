package utils

import "os"

func GetScopeReplication() string {
	scope := os.Getenv("target_scope")
	if scope == "" {
		return "private"
	}
	return scope
}
