//go:build !linux

package main

import "errors"

func sandbox() error { return errors.New("artifact parser sandbox requires Linux") }
