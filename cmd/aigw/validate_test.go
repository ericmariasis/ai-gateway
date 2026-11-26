// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	const basicConfig = "testdata/translate_basic.in.yaml"
	config, err := os.ReadFile(basicConfig)
	require.NoError(t, err)

	// Change only the route's parent kind. The Gateway and all backends still exist.
	gatewayParent := []byte("      kind: Gateway")
	require.Equal(t, 1, bytes.Count(config, gatewayParent))
	dir := t.TempDir()
	writeParentVariant := func(name, replacement string) string {
		path := filepath.Join(dir, name)
		variant := bytes.Replace(config, gatewayParent, []byte(replacement), 1)
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
			paths:  []string{writeParentVariant("httproute-parent.yaml", "      kind: HTTPRoute")},
			expErr: "only Gateway is supported",
		},
		{
			name:   "omitted parent kind defaults to Gateway",
			paths:  []string{writeParentVariant("omitted-parent-kind.yaml", "")},
			expOut: "OK: config is valid\n",
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
