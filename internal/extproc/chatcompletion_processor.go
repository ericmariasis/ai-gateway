// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/envoyproxy/ai-gateway/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/extproc/translator"
	"github.com/envoyproxy/ai-gateway/internal/llmcostcel"
)

// NewChatCompletionProcessor implements [ProcessorIface] for the /chat/completions endpoint.
func NewChatCompletionProcessor(config *processorConfig, requestHeaders map[string]string, logger *slog.Logger) ProcessorIface {
	return &chatCompletionProcessor{
		config:         config,
		requestHeaders: requestHeaders,
		logger:         logger,
	}
}

// chatCompletionProcessor handles the processing of the request and response messages for a single stream.
type chatCompletionProcessor struct {
	logger           *slog.Logger
	config           *processorConfig
	requestHeaders   map[string]string
	responseHeaders  map[string]string
	responseEncoding string
	translator       translator.Translator
	// cost is the cost of the request that is accumulated during the processing of the response.
	costs translator.LLMTokenUsage
}

// selectTranslator selects the translator based on the output schema.
func (c *chatCompletionProcessor) selectTranslator(out filterapi.VersionedAPISchema) error {
	if c.translator != nil { // Prevents re-selection and allows translator injection in tests.
		return nil
	}
	// TODO: currently, we ignore the LLMAPISchema."Version" field.
	switch out.Name {
	case filterapi.APISchemaOpenAI:
		c.translator = translator.NewChatCompletionOpenAIToOpenAITranslator()
	case filterapi.APISchemaAWSBedrock:
		c.translator = translator.NewChatCompletionOpenAIToAWSBedrockTranslator()
	default:
		return fmt.Errorf("unsupported API schema: backend=%s", out)
	}
	return nil
}

// ProcessRequestHeaders implements [ProcessorIface.ProcessRequestHeaders].
func (c *chatCompletionProcessor) ProcessRequestHeaders(_ context.Context, _ *corev3.HeaderMap) (res *extprocv3.ProcessingResponse, err error) {
	// The request headers have already been at the time the processor was created
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{
		RequestHeaders: &extprocv3.HeadersResponse{},
	}}, nil
}

// ProcessRequestBody implements [ProcessorIface.ProcessRequestBody].
func (c *chatCompletionProcessor) ProcessRequestBody(ctx context.Context, rawBody *extprocv3.HttpBody) (res *extprocv3.ProcessingResponse, err error) {
	path := c.requestHeaders[":path"]
	model, body, err := c.config.bodyParser(path, rawBody)
	if err != nil {
		return nil, fmt.Errorf("failed to parse request body: %w", err)
	}
	c.logger.Info("Processing request", "path", path, "model", model)

	c.requestHeaders[c.config.modelNameHeaderKey] = model
	b, err := c.config.router.Calculate(c.requestHeaders)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate route: %w", err)
	}
	c.logger.Info("Selected backend", "backend", b.Name)

	if err = c.selectTranslator(b.Schema); err != nil {
		return nil, fmt.Errorf("failed to select translator: %w", err)
	}

	headerMutation, bodyMutation, override, err := c.translator.RequestBody(body)
	if err != nil {
		return nil, fmt.Errorf("failed to transform request: %w", err)
	}

	if headerMutation == nil {
		headerMutation = &extprocv3.HeaderMutation{}
	}
	// Set the model name to the request header with the key `x-ai-gateway-llm-model-name`.
	headerMutation.SetHeaders = append(headerMutation.SetHeaders, &corev3.HeaderValueOption{
		Header: &corev3.HeaderValue{Key: c.config.modelNameHeaderKey, RawValue: []byte(model)},
	}, &corev3.HeaderValueOption{
		Header: &corev3.HeaderValue{Key: c.config.selectedBackendHeaderKey, RawValue: []byte(b.Name)},
	})

	if authHandler, ok := c.config.backendAuthHandlers[b.Name]; ok {
		if err := authHandler.Do(ctx, c.requestHeaders, headerMutation, bodyMutation); err != nil {
			return nil, fmt.Errorf("failed to do auth request: %w", err)
		}
	}

	resp := &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestBody{
			RequestBody: &extprocv3.BodyResponse{
				Response: &extprocv3.CommonResponse{
					HeaderMutation:  headerMutation,
					BodyMutation:    bodyMutation,
					ClearRouteCache: true,
				},
			},
		},
		ModeOverride: override,
	}
	return resp, nil
}

// ProcessResponseHeaders implements [ProcessorIface.ProcessResponseHeaders].
func (c *chatCompletionProcessor) ProcessResponseHeaders(_ context.Context, headers *corev3.HeaderMap) (res *extprocv3.ProcessingResponse, err error) {
	c.responseHeaders = headersToMap(headers)
	if enc := c.responseHeaders["content-encoding"]; enc != "" {
		c.responseEncoding = enc
	}
	// The translator can be nil as there could be response event generated by previous ext proc without
	// getting the request event.
	if c.translator == nil {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{
			ResponseHeaders: &extprocv3.HeadersResponse{},
		}}, nil
	}
	headerMutation, err := c.translator.ResponseHeaders(c.responseHeaders)
	if err != nil {
		return nil, fmt.Errorf("failed to transform response headers: %w", err)
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{
		ResponseHeaders: &extprocv3.HeadersResponse{
			Response: &extprocv3.CommonResponse{HeaderMutation: headerMutation},
		},
	}}, nil
}

// ProcessResponseBody implements [ProcessorIface.ProcessResponseBody].
func (c *chatCompletionProcessor) ProcessResponseBody(_ context.Context, body *extprocv3.HttpBody) (res *extprocv3.ProcessingResponse, err error) {
	var br io.Reader
	switch c.responseEncoding {
	case "gzip":
		br, err = gzip.NewReader(bytes.NewReader(body.Body))
		if err != nil {
			return nil, fmt.Errorf("failed to decode gzip: %w", err)
		}
	default:
		br = bytes.NewReader(body.Body)
	}
	// The translator can be nil as there could be response event generated by previous ext proc without
	// getting the request event.
	if c.translator == nil {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{}}, nil
	}

	headerMutation, bodyMutation, tokenUsage, err := c.translator.ResponseBody(c.responseHeaders, br, body.EndOfStream)
	if err != nil {
		return nil, fmt.Errorf("failed to transform response: %w", err)
	}

	resp := &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_ResponseBody{
			ResponseBody: &extprocv3.BodyResponse{
				Response: &extprocv3.CommonResponse{
					HeaderMutation: headerMutation,
					BodyMutation:   bodyMutation,
				},
			},
		},
	}

	// TODO: this is coupled with "LLM" specific logic. Once we have another use case, we need to refactor this.
	c.costs.InputTokens += tokenUsage.InputTokens
	c.costs.OutputTokens += tokenUsage.OutputTokens
	c.costs.TotalTokens += tokenUsage.TotalTokens
	if body.EndOfStream && len(c.config.requestCosts) > 0 {
		resp.DynamicMetadata, err = c.maybeBuildDynamicMetadata()
		if err != nil {
			return nil, fmt.Errorf("failed to build dynamic metadata: %w", err)
		}
	}
	return resp, nil
}

func (c *chatCompletionProcessor) maybeBuildDynamicMetadata() (*structpb.Struct, error) {
	metadata := make(map[string]*structpb.Value, len(c.config.requestCosts))
	for i := range c.config.requestCosts {
		rc := &c.config.requestCosts[i]
		var cost uint32
		switch rc.Type {
		case filterapi.LLMRequestCostTypeInputToken:
			cost = c.costs.InputTokens
		case filterapi.LLMRequestCostTypeOutputToken:
			cost = c.costs.OutputTokens
		case filterapi.LLMRequestCostTypeTotalToken:
			cost = c.costs.TotalTokens
		case filterapi.LLMRequestCostTypeCELExpression:
			costU64, err := llmcostcel.EvaluateProgram(
				rc.celProg,
				c.requestHeaders[c.config.modelNameHeaderKey],
				c.requestHeaders[c.config.selectedBackendHeaderKey],
				c.costs.InputTokens,
				c.costs.OutputTokens,
				c.costs.TotalTokens,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate CEL expression: %w", err)
			}
			cost = uint32(costU64) //nolint:gosec
		default:
			return nil, fmt.Errorf("unknown request cost kind: %s", rc.Type)
		}
		c.logger.Info("Setting request cost metadata", "type", rc.Type, "cost", cost, "metadataKey", rc.MetadataKey)
		metadata[rc.MetadataKey] = &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(cost)}}
	}
	if len(metadata) == 0 {
		return nil, nil
	}
	return &structpb.Struct{
		Fields: map[string]*structpb.Value{
			c.config.metadataNamespace: {
				Kind: &structpb.Value_StructValue{
					StructValue: &structpb.Struct{Fields: metadata},
				},
			},
		},
	}, nil
}
