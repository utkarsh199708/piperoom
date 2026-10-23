package kafkatools

// SensitiveKey exposes sensitiveKey to the external tests: they import
// package actions, which imports this package (CleanTaskTrace), so they
// cannot live in package kafkatools.
var SensitiveKey = sensitiveKey

// LooksSecretKey exposes the validator's private copy of the shared
// actions.LooksSecret rule, so the external drift-pin test can compare the
// two (the copy exists because actions imports this package; it goes away
// when a leaf package owns the rule, Q0/U1-23).
var LooksSecretKey = looksSecretKey
