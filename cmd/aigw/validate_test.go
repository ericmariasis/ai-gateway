// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	const basicConfig = "testdata/translate_basic.in.yaml"
	const mcpConfig = "testdata/validate_mcp_parent.yaml"

	const gatewayKind = "      kind: Gateway"
	const parentRefs = `  parentRefs:
    - name: %s
      kind: Gateway
      group: gateway.networking.k8s.io
`
	aiParents := fmt.Sprintf(parentRefs, "envoy-ai-gateway-basic")
	mcpParents := fmt.Sprintf(parentRefs, "aigw-run")
	const missingMCPParents = `MCPRoute "mcp-route" spec.parentRefs must contain at least one Gateway reference`

	// Change only the route's parent references. Keep its Gateway and backends in the input.
	writeParentVariant := func(configPath, original, replacement string) string {
		t.Helper()
		config, err := os.ReadFile(configPath)
		require.NoError(t, err)
		config = bytes.ReplaceAll(config, []byte("\r\n"), []byte("\n"))
		require.Equal(t, 1, bytes.Count(config, []byte(original)))
		path := filepath.Join(t.TempDir(), filepath.Base(configPath))
		variant := bytes.Replace(config, []byte(original), []byte(replacement), 1)
		require.NoError(t, os.WriteFile(path, variant, 0o600))
		return path
	}

	for _, tc := range []struct {
		name   string
		paths  []string
		expErr string
		expOut string
	}{
		{
			name:   "valid basic config",
			paths:  []string{basicConfig},
			expOut: "OK: config is valid\n",
		},
		{
			name:   "HTTPRoute parent kind with existing Gateway and backends",
			paths:  []string{writeParentVariant(basicConfig, gatewayKind, "      kind: HTTPRoute")},
			expErr: "only Gateway is supported",
		},
		{
			name:   "omitted parent kind defaults to Gateway",
			paths:  []string{writeParentVariant(basicConfig, gatewayKind, "")},
			expOut: "OK: config is valid\n",
		},
		{
			name:   "AIGatewayRoute with omitted parentRefs",
			paths:  []string{writeParentVariant(basicConfig, aiParents, "")},
			expOut: "OK: config is valid\n",
		},
		{
			name:   "MCPRoute with Gateway parent",
			paths:  []string{mcpConfig},
			expOut: "OK: config is valid\n",
		},
		{
			name:   "MCPRoute with omitted parent kind",
			paths:  []string{writeParentVariant(mcpConfig, gatewayKind, "")},
			expOut: "OK: config is valid\n",
		},
		{
			name:   "MCPRoute with HTTPRoute parent",
			paths:  []string{writeParentVariant(mcpConfig, gatewayKind, "      kind: HTTPRoute")},
			expErr: `MCPRoute "mcp-route" spec.parentRefs[0].kind "HTTPRoute": only Gateway is supported`,
		},
		{
			name:   "MCPRoute with empty parentRefs",
			paths:  []string{writeParentVariant(mcpConfig, mcpParents, "  parentRefs: []\n")},
			expErr: missingMCPParents,
		},
		{
			name:   "MCPRoute with omitted parentRefs",
			paths:  []string{writeParentVariant(mcpConfig, mcpParents, "")},
			expErr: missingMCPParents,
		},
		{
			name:   "invalid parentRef kind",
			paths:  []string{"testdata/validate_invalid_parent_ref.yaml"},
			expErr: "config validation failed",
		},
		{
			name:   "file not found",
			paths:  []string{"testdata/does_not_exist.yaml"},
			expErr: "error reading file",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := validate(context.Background(), &cmdValidate{Paths: tc.paths}, &stdout, &stderr)
			if tc.expErr != "" {
				require.ErrorContains(t, err, tc.expErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expOut, stdout.String())
			}
		})
	}
}
