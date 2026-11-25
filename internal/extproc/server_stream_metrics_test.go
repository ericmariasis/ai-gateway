// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/envoyproxy/ai-gateway/internal/endpointspec"
	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	"github.com/envoyproxy/ai-gateway/internal/metrics"
	"github.com/envoyproxy/ai-gateway/internal/tracing/tracingapi"
)

func TestServer_Process_UpstreamCloseBeforeResponse(t *testing.T) {
	const path = "/v1/chat/completions"
	const request = `{"model":"test-model","messages":[{"role":"user","content":"hello"}],"stream":true,"stream_options":{"include_usage":true}}`
	const usage = `data: {"model":"test-model","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}` + "\n\n"
	for _, tc := range []struct {
		name          string
		sendFinalBody bool
		wantErrorType string
	}{
		{name: "normal completion", sendFinalBody: true},
		{name: "early close", wantErrorType: "_OTHER"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
			factory := metrics.NewMetricsFactory(provider.Meter("test"), nil, metrics.GenAIOperationChat)
			server, err := NewServer(slog.Default(), false)
			require.NoError(t, err)
			require.NoError(t, server.LoadConfig(t.Context(), &filterapi.Config{Backends: []filterapi.Backend{
				{Name: "openai", Schema: filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI, Prefix: "v1"}},
			}}))
			server.Register(path, NewFactory(factory, tracingapi.NoopChatCompletionTracer{}, endpointspec.ChatCompletionsEndpointSpec{}))

			client := newStreamMetricsClient(t, server)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			router, err := client.Process(ctx)
			require.NoError(t, err)
			upstream, err := client.Process(ctx)
			require.NoError(t, err)
			exchange := func(stream extprocv3.ExternalProcessor_ProcessClient, req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
				t.Helper()
				require.NoError(t, stream.Send(req))
				resp, err := stream.Recv()
				require.NoError(t, err)
				require.Nil(t, resp.GetImmediateResponse())
				return resp
			}
			closeStream := func(stream extprocv3.ExternalProcessor_ProcessClient) {
				t.Helper()
				require.NoError(t, stream.CloseSend())
				_, err := stream.Recv() // Wait until Server.Process has finished teardown.
				require.ErrorIs(t, err, io.EOF)
			}

			routerHeaders := &extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestHeaders{
				RequestHeaders: &extprocv3.HttpHeaders{Headers: &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: ":path", Value: path}}}},
			}}
			headersReply := exchange(router, routerHeaders)
			bodyReply := exchange(router, &extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestBody{
				RequestBody: &extprocv3.HttpBody{Body: []byte(request), EndOfStream: true},
			}})
			// Forward the router's request ID, original path and model to the upstream filter.
			upstreamHeaders := proto.CloneOf(routerHeaders)
			h := upstreamHeaders.GetRequestHeaders().GetHeaders()
			for _, mutation := range []*extprocv3.HeaderMutation{
				headersReply.GetRequestHeaders().GetResponse().GetHeaderMutation(),
				bodyReply.GetRequestBody().GetResponse().GetHeaderMutation(),
			} {
				for _, header := range mutation.GetSetHeaders() {
					h.Headers = append(h.Headers, header.GetHeader())
				}
			}
			upstreamHeaders.Attributes = map[string]*structpb.Struct{"envoy.filters.http.ext_proc": {Fields: map[string]*structpb.Value{
				internalapi.XDSUpstreamHostMetadataBackendNamePath: structpb.NewStringValue("openai"),
			}}}
			exchange(upstream, upstreamHeaders)
			closeStream(upstream)
			_, completions := streamMetricCounts(t, reader)
			assert.Empty(t, completions, "upstream EOF while the router is still active")

			exchange(router, &extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseHeaders{
				ResponseHeaders: &extprocv3.HttpHeaders{Headers: &corev3.HeaderMap{Headers: []*corev3.HeaderValue{
					{Key: ":status", Value: "200"}, {Key: "content-type", Value: "text/event-stream"},
				}}},
			}})
			sendBody := func(data string, final bool) {
				t.Helper()
				exchange(router, &extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_ResponseBody{
					ResponseBody: &extprocv3.HttpBody{Body: []byte(data), EndOfStream: final},
				}})
			}
			sendBody(usage, false)
			if tc.sendFinalBody {
				sendBody("data: [DONE]\n\n", false)
				sendBody("", true)
			}
			closeStream(router)
			tokens, completions := streamMetricCounts(t, reader)
			assert.Equal(t, map[string]tokenSamples{"input": {count: 1, sum: 10}, "output": {count: 1, sum: 5}}, tokens)
			assert.Equal(t, map[string]uint64{tc.wantErrorType: 1}, completions)
		})
	}
}

func newStreamMetricsClient(t *testing.T, server *Server) extprocv3.ExternalProcessorClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	extprocv3.RegisterExternalProcessorServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient("passthrough:///extproc",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return extprocv3.NewExternalProcessorClient(conn)
}

type tokenSamples struct {
	count uint64
	sum   float64
}

// Group across response models: a failure before the first chunk can have an unknown model.
func streamMetricCounts(t *testing.T, reader *sdkmetric.ManualReader) (map[string]tokenSamples, map[string]uint64) {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	tokens := make(map[string]tokenSamples)
	completions := make(map[string]uint64)
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch m.Name {
			case "gen_ai.client.token.usage":
				for _, point := range m.Data.(metricdata.Histogram[float64]).DataPoints {
					kind, _ := point.Attributes.Value("gen_ai.token.type")
					samples := tokens[kind.AsString()]
					samples.count += point.Count
					samples.sum += point.Sum
					tokens[kind.AsString()] = samples
				}
			case "gen_ai.server.request.duration":
				for _, point := range m.Data.(metricdata.Histogram[float64]).DataPoints {
					errorType, _ := point.Attributes.Value("error.type")
					completions[errorType.AsString()] += point.Count
				}
			}
		}
	}
	return tokens, completions
}
