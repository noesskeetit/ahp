package main

import "errors"

// errFeatureNotBuilt signals that an optional role (execution / data) was
// requested but the binary was built without its build tag. serveAll treats it
// as a non-fatal skip, so the default build and the other roles are never
// broken by the optional execution / data planes.
var errFeatureNotBuilt = errors.New("ahp: role not built (rebuild with the matching build tag)")
