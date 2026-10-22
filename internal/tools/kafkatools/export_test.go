package kafkatools

// SensitiveKey exposes sensitiveKey to the external tests: they import
// package actions, which imports this package (CleanTaskTrace), so they
// cannot live in package kafkatools.
var SensitiveKey = sensitiveKey
