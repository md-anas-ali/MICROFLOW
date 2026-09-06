//go:build !linux

package main

// disableEcho has no non-Linux implementation here (kept dependency-free
// on purpose). setcred still works on other platforms -- input just
// isn't hidden; use the MICROFLOW_CLIENT_SECRET / MICROFLOW_REFRESH_TOKEN
// env vars there instead if terminal echo is a concern.
func disableEcho(fd uintptr) (restore func(), ok bool) {
	return nil, false
}
