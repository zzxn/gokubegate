package gokubegate

import "errors"

// ErrNoEndpoints is returned by RoundTrip when the target service currently
// has no ready endpoints. Detect it with errors.Is.
var ErrNoEndpoints = errors.New("gokubegate: no ready endpoints for service")
