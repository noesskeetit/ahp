// ahp-sdk-check verifies custom agent code against the AHP platform contract.
// Usage: ahp-sdk-check [path...]
// Exit code 1 if any ERROR-severity violation is found.
package main

import (
	"fmt"
	"os"

	"github.com/ahp-platform/ahp/pkg/sdk/check"
)

func main() {
	roots := os.Args[1:]
	if len(roots) == 0 {
		roots = []string{"."}
	}
	var totalFiles int
	var allViolations []check.Violation
	for _, root := range roots {
		result, err := check.CheckAll(root)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ahp-sdk check: %v\n", err)
			os.Exit(2)
		}
		totalFiles += result.Files
		allViolations = append(allViolations, result.Violations...)
	}
	hasErrors := false
	for _, v := range allViolations {
		fmt.Printf("%s:%d [%s] %s: %s\n", v.File, v.Line, v.Severity, v.Rule, v.Message)
		if v.Severity == check.Error {
			hasErrors = true
		}
	}
	fmt.Printf("\nScanned %d files, %d violations.\n", totalFiles, len(allViolations))
	if hasErrors {
		os.Exit(1)
	}
}
