// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package router

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/envoyproxy/ai-gateway/filterapi"
	"github.com/envoyproxy/ai-gateway/filterapi/x"
)

// dummyCustomRouter implements [filterapi.Router].
type dummyCustomRouter struct{ called bool }

func (c *dummyCustomRouter) Calculate(map[string]string) (*filterapi.Backend, error) {
	c.called = true
	return nil, nil
}

func TestRouter_NewRouter_Custom(t *testing.T) {
	r, err := NewRouter(&filterapi.Config{}, func(defaultRouter x.Router, _ *filterapi.Config) x.Router {
		require.NotNil(t, defaultRouter)
		_, ok := defaultRouter.(*router)
		require.True(t, ok) // Checking if the default router is correctly passed.
		return &dummyCustomRouter{}
	})
	require.NoError(t, err)
	_, ok := r.(*dummyCustomRouter)
	require.True(t, ok)

	_, err = r.Calculate(nil)
	require.NoError(t, err)
	require.True(t, r.(*dummyCustomRouter).called)
}

func TestRouter_Calculate(t *testing.T) {
	outSchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}
	_r, err := NewRouter(&filterapi.Config{
		Rules: []filterapi.RouteRule{
			{
				Backends: []filterapi.Backend{
					{Name: "foo", Schema: outSchema, Weight: 1},
					{Name: "bar", Schema: outSchema, Weight: 3},
				},
				Headers: []filterapi.HeaderMatch{
					{Name: "x-model-name", Value: "llama3.3333"},
				},
			},
			{
				Backends: []filterapi.Backend{
					{Name: "openai", Schema: outSchema},
				},
				Headers: []filterapi.HeaderMatch{
					{Name: "x-model-name", Value: "gpt4.4444"},
				},
			},
		},
	}, nil)
	require.NoError(t, err)
	r, ok := _r.(*router)
	require.True(t, ok)

	t.Run("no matching rule", func(t *testing.T) {
		b, err := r.Calculate(map[string]string{"x-model-name": "something-quirky"})
		require.Error(t, err)
		require.Nil(t, b)
	})
	t.Run("matching rule - single backend choice", func(t *testing.T) {
		b, err := r.Calculate(map[string]string{"x-model-name": "gpt4.4444"})
		require.NoError(t, err)
		require.Equal(t, "openai", b.Name)
		require.Equal(t, outSchema, b.Schema)
	})
	t.Run("matching rule - multiple backend choices", func(t *testing.T) {
		chosenNames := make(map[string]int)
		for i := 0; i < 1000; i++ {
			b, err := r.Calculate(map[string]string{"x-model-name": "llama3.3333"})
			require.NoError(t, err)
			chosenNames[b.Name]++
			require.Contains(t, []string{"foo", "bar"}, b.Name)
			require.Equal(t, outSchema, b.Schema)
		}
		require.Greater(t, chosenNames["bar"], chosenNames["foo"])
		require.Greater(t, chosenNames["bar"], 700)
		require.Greater(t, chosenNames["foo"], 200)
	})
}

func TestRouter_selectBackendFromRule(t *testing.T) {
	_r, err := NewRouter(&filterapi.Config{}, nil)
	require.NoError(t, err)
	r, ok := _r.(*router)
	require.True(t, ok)

	outSchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}

	rule := &filterapi.RouteRule{
		Backends: []filterapi.Backend{
			{Name: "foo", Schema: outSchema, Weight: 1},
			{Name: "bar", Schema: outSchema, Weight: 3},
		},
	}

	chosenNames := make(map[string]int)
	for i := 0; i < 1000; i++ {
		b := r.selectBackendFromRule(rule)
		chosenNames[b.Name]++
	}

	require.Greater(t, chosenNames["bar"], chosenNames["foo"])
	require.Greater(t, chosenNames["bar"], 700)
	require.Greater(t, chosenNames["foo"], 200)
}
