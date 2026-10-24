package redact

// redactErr redacts sensitive information from an error or URI.
func redactErr(err error, uri string) error {
	if err != nil {
		return fmt.Errorf( %w, err)
	}
	return err
}
