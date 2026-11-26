// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

//go:build test_extproc

package dataplane

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/envoyproxy/ai-gateway/internal/idcodec"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	"github.com/envoyproxy/ai-gateway/tests/internal/testupstreamlib"
)

// encodeGatewayBatchID mints a gateway batch id for the files test backend.
func encodeGatewayBatchID(t *testing.T, nativeID string) string {
	t.Helper()
	ns, name, ok := internalapi.NamespaceAndNameFromBackendName(filesTestBackendName1)
	require.True(t, ok)
	id, err := newFilesTestCodec().Encode(&idcodec.BackendID{
		Namespace: ns, Name: name, Kind: idcodec.KindBatch, Model: filesTestModel, NativeID: nativeID,
	})
	require.NoError(t, err)
	return id
}

// batchRequest describes one gateway request and the test upstream's expectations/response.
type batchRequest struct {
	method, path string
	body         string
	// upstream expectations / canned response.
	expPath, expQuery, expBody   string
	responseBody, responseStatus string
}

// doBatchRequest sends the request (retrying until expStatus, as Envoy may still be warming up)
// and returns the final response body.
func doBatchRequest(t *testing.T, addr string, r *batchRequest, expStatus int) []byte {
	t.Helper()
	var lastBody []byte
	var lastStatus int
	// assert.Eventually evaluates its message arguments before polling, so they would always show the
	// initial zero values. Poll without failing, then assert on the captured final state instead.
	assert.Eventually(&noopT{}, func() bool {
		req, err := http.NewRequestWithContext(t.Context(), r.method, addr+r.path, strings.NewReader(r.body))
		if err != nil {
			return false
		}
		if r.body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		setExpectedFilesAuthHeader(req)
		if r.responseBody != "" {
			req.Header.Set(testupstreamlib.ResponseBodyHeaderKey, base64.StdEncoding.EncodeToString([]byte(r.responseBody)))
		}
		if r.responseStatus != "" {
			req.Header.Set(testupstreamlib.ResponseStatusKey, r.responseStatus)
		}
		if r.expPath != "" {
			req.Header.Set(testupstreamlib.ExpectedPathHeaderKey, base64.StdEncoding.EncodeToString([]byte(r.expPath)))
		}
		if r.expQuery != "" {
			req.Header.Set(testupstreamlib.ExpectedRawQueryHeaderKey, r.expQuery)
		}
		if r.expBody != "" {
			req.Header.Set(testupstreamlib.ExpectedRequestBodyHeaderKey, base64.StdEncoding.EncodeToString([]byte(r.expBody)))
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastBody = []byte("client error: " + err.Error())
			return false
		}
		defer resp.Body.Close()
		lastBody, _ = io.ReadAll(resp.Body)
		lastStatus = resp.StatusCode
		return lastStatus == expStatus
	}, eventuallyTimeout, eventuallyInterval)
	require.Equal(t, expStatus, lastStatus, "body: %s", lastBody)
	return lastBody
}

// noopT discards assertion failures so the polling loop itself never fails the test.
type noopT struct{}

func (*noopT) Errorf(string, ...any) {}

// requireDecodes asserts that id is a gateway id of the given kind wrapping nativeID.
func requireDecodes(t *testing.T, id, kind, nativeID string) {
	t.Helper()
	d, err := newFilesTestCodec().Decode(id)
	require.NoError(t, err, "id %q", id)
	require.Equal(t, kind, d.Kind)
	require.Equal(t, nativeID, d.NativeID)
	require.Equal(t, filesTestModel, d.Model)
}

// TestBatchAPI_Lifecycle walks the full flow against the test upstream:
// upload input file -> create batch -> retrieve -> download output and errors -> list (+ paginate) -> cancel.
func TestBatchAPI_Lifecycle(t *testing.T) {
	env := startTestEnvironment(t, filesTestConfig(), true, false)
	addr := fmt.Sprintf("http://localhost:%d", env.EnvoyListenerPort())

	// 1. Upload the input file through the Files API to obtain a gateway file id.
	const inputJSONL = `{"custom_id":"row-success","method":"POST","url":"/v1/chat/completions","body":{"model":"` + filesTestModel + `","messages":[{"role":"user","content":"Is the café open?"}]}}` + "\n" +
		`{"custom_id":"row-failure","method":"POST","url":"/v1/chat/completions","body":{"model":"` + filesTestModel + `","messages":[{"role":"user","content":"What time does the shop open?"}]}}` + "\n"
	uploadBody, ct := buildUploadBodyWithContent(t, filesTestModel, inputJSONL)
	var inputFileID string
	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, addr+"/v1/files", bytes.NewReader(uploadBody))
		require.NoError(t, err)
		req.Header.Set("Content-Type", ct)
		setExpectedFilesAuthHeader(req)
		req.Header.Set(testupstreamlib.ResponseBodyHeaderKey, base64.StdEncoding.EncodeToString(
			[]byte(fmt.Sprintf(`{"id":"file-in-native","object":"file","bytes":%d,"purpose":"batch"}`, len(inputJSONL)))))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		inputFileID = gjson.GetBytes(b, "id").String()
		return resp.StatusCode == http.StatusOK
	}, eventuallyTimeout, eventuallyInterval)
	requireDecodes(t, inputFileID, idcodec.KindFile, "file-in-native")

	nativeBatch := `{"id":"batch_native_1","object":"batch","endpoint":"/v1/chat/completions",` +
		`"input_file_id":"file-in-native","completion_window":"24h","status":"%s",` +
		`"output_file_id":%s,"error_file_id":%s,"created_at":1,"request_counts":{"total":2,"completed":%d,"failed":%d}}`
	completedBatch := fmt.Sprintf(nativeBatch, "completed", `"file-out-native"`, `"file-err-native"`, 1, 1)

	// 2. Create: routed by the input file's model; the upstream sees the native file id.
	created := doBatchRequest(t, addr, &batchRequest{
		method:       http.MethodPost,
		path:         "/v1/batches",
		body:         `{"input_file_id":"` + inputFileID + `","endpoint":"/v1/chat/completions","completion_window":"24h"}`,
		expPath:      "/v1/batches",
		expBody:      `{"input_file_id":"file-in-native","endpoint":"/v1/chat/completions","completion_window":"24h"}`,
		responseBody: fmt.Sprintf(nativeBatch, "validating", "null", "null", 0, 0),
	}, http.StatusOK)
	batchID := gjson.GetBytes(created, "id").String()
	requireDecodes(t, batchID, idcodec.KindBatch, "batch_native_1")
	require.Equal(t, inputFileID, gjson.GetBytes(created, "input_file_id").String())
	require.NotContains(t, string(created), "native")

	// 3. Retrieve a completed batch with one successful request and one failed request.
	retrieved := doBatchRequest(t, addr, &batchRequest{
		method:       http.MethodGet,
		path:         "/v1/batches/" + batchID,
		expPath:      "/v1/batches/batch_native_1",
		responseBody: completedBatch,
	}, http.StatusOK)
	require.Equal(t, batchID, gjson.GetBytes(retrieved, "id").String())
	require.Equal(t, "completed", gjson.GetBytes(retrieved, "status").String())
	require.Equal(t, int64(2), gjson.GetBytes(retrieved, "request_counts.total").Int())
	require.Equal(t, int64(1), gjson.GetBytes(retrieved, "request_counts.completed").Int())
	require.Equal(t, int64(1), gjson.GetBytes(retrieved, "request_counts.failed").Int())

	// 4. Each returned file id must reach its own upstream path and preserve the JSONL bytes.
	for _, file := range []struct {
		field, nativeID, jsonl string
	}{
		{
			field: "output_file_id", nativeID: "file-out-native",
			jsonl: `{"id":"batch_req_001","custom_id":"row-success","response":{"status_code":200,"request_id":"req_001",` +
				`"body":{"object":"chat.completion","model":"` + filesTestModel + `","choices":[{"index":0,` +
				`"message":{"role":"assistant","content":"Café is open."},"finish_reason":"stop"}]}},"error":null}` + "\n",
		},
		{
			field: "error_file_id", nativeID: "file-err-native",
			jsonl: `{"id":"batch_req_002","custom_id":"row-failure","response":{"status_code":503,"request_id":"req_002",` +
				`"body":{"error":{"type":"server_error","code":"service_unavailable","message":"Provider temporarily unavailable."}}},"error":null}` + "\n",
		},
	} {
		t.Run(file.field, func(t *testing.T) {
			fileID := gjson.GetBytes(retrieved, file.field).String()
			requireDecodes(t, fileID, idcodec.KindFile, file.nativeID)
			content := doBatchRequest(t, addr, &batchRequest{
				method:       http.MethodGet,
				path:         "/v1/files/" + fileID + "/content",
				expPath:      "/v1/files/" + file.nativeID + "/content",
				responseBody: file.jsonl,
			}, http.StatusOK)
			require.Equal(t, []byte(file.jsonl), content)
		})
	}

	// 5. List by model; "model" is stripped and ids/cursors are gateway-encoded.
	listed := doBatchRequest(t, addr, &batchRequest{
		method:       http.MethodGet,
		path:         "/v1/batches?model=" + filesTestModel + "&limit=1",
		expPath:      "/v1/batches",
		expQuery:     "limit=1",
		responseBody: `{"object":"list","data":[` + completedBatch + `],"first_id":"batch_native_1","last_id":"batch_native_1","has_more":true}`,
	}, http.StatusOK)
	require.Equal(t, batchID, gjson.GetBytes(listed, "data.0.id").String())
	lastID := gjson.GetBytes(listed, "last_id").String()
	require.Equal(t, batchID, lastID)
	require.NotContains(t, string(listed), "native")

	// 6. Paginate with the gateway cursor only (stock SDK behaviour, no ?model=).
	doBatchRequest(t, addr, &batchRequest{
		method:       http.MethodGet,
		path:         "/v1/batches?limit=1&after=" + lastID,
		expPath:      "/v1/batches",
		expQuery:     "after=batch_native_1&limit=1",
		responseBody: `{"object":"list","data":[],"has_more":false}`,
	}, http.StatusOK)

	// 7. Cancel.
	cancelled := doBatchRequest(t, addr, &batchRequest{
		method:       http.MethodPost,
		path:         "/v1/batches/" + batchID + "/cancel",
		expPath:      "/v1/batches/batch_native_1/cancel",
		responseBody: fmt.Sprintf(nativeBatch, "cancelling", "null", "null", 0, 0),
	}, http.StatusOK)
	require.Equal(t, "cancelling", gjson.GetBytes(cancelled, "status").String())
	require.Equal(t, batchID, gjson.GetBytes(cancelled, "id").String())
}

// TestBatchAPI_Rejections covers the gateway-side validation errors.
func TestBatchAPI_Rejections(t *testing.T) {
	env := startTestEnvironment(t, filesTestConfig(), true, false)
	addr := fmt.Sprintf("http://localhost:%d", env.EnvoyListenerPort())

	fileID := encodeGatewayID(t, "test-ns", "files-backend", "file-native-abc")
	batchID := encodeGatewayBatchID(t, "batch_native_1")

	for _, tc := range []struct {
		name      string
		req       batchRequest
		expStatus int
	}{
		{"create - native input_file_id", batchRequest{
			method: http.MethodPost, path: "/v1/batches",
			body: `{"input_file_id":"file-abc","endpoint":"/v1/chat/completions","completion_window":"24h"}`,
		}, http.StatusBadRequest},
		{"create - batch id as input file", batchRequest{
			method: http.MethodPost, path: "/v1/batches",
			body: `{"input_file_id":"` + batchID + `","endpoint":"/v1/chat/completions","completion_window":"24h"}`,
		}, http.StatusBadRequest},
		{"list - missing model", batchRequest{method: http.MethodGet, path: "/v1/batches"}, http.StatusBadRequest},
		{"list - file id as cursor", batchRequest{method: http.MethodGet, path: "/v1/batches?model=" + filesTestModel + "&after=" + fileID}, http.StatusBadRequest},
		{"retrieve - forged id", batchRequest{method: http.MethodGet, path: "/v1/batches/batch-forged"}, http.StatusNotFound},
		{"retrieve - file id", batchRequest{method: http.MethodGet, path: "/v1/batches/" + fileID}, http.StatusNotFound},
		{"unsupported method", batchRequest{method: http.MethodDelete, path: "/v1/batches/" + batchID}, http.StatusNotFound},
		{"upstream error passes through", batchRequest{
			method: http.MethodGet, path: "/v1/batches/" + batchID,
			expPath: "/v1/batches/batch_native_1", responseStatus: "404",
			responseBody: `{"error":{"message":"No batch found with id 'batch_native_1'.","type":"invalid_request_error"}}`,
		}, http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := doBatchRequest(t, addr, &tc.req, tc.expStatus)
			require.Contains(t, string(body), "error")
		})
	}
}

// TestBatchAPI_OpenAIAcceptsAnyEndpoint verifies that endpoint support is decided by the backend's
// translator: the OpenAI translator forwards every batch endpoint, with the native input file id.
func TestBatchAPI_OpenAIAcceptsAnyEndpoint(t *testing.T) {
	env := startTestEnvironment(t, filesTestConfig(), true, false)
	addr := fmt.Sprintf("http://localhost:%d", env.EnvoyListenerPort())
	fileID := encodeGatewayID(t, "test-ns", "files-backend", "file-native-abc")

	for _, endpoint := range []string{"/v1/images/generations", "/v1/moderations"} {
		t.Run(endpoint, func(t *testing.T) {
			created := doBatchRequest(t, addr, &batchRequest{
				method:  http.MethodPost,
				path:    "/v1/batches",
				body:    `{"input_file_id":"` + fileID + `","endpoint":"` + endpoint + `","completion_window":"24h"}`,
				expPath: "/v1/batches",
				expBody: `{"input_file_id":"file-native-abc","endpoint":"` + endpoint + `","completion_window":"24h"}`,
				responseBody: `{"id":"batch_native_2","object":"batch","endpoint":"` + endpoint +
					`","input_file_id":"file-native-abc","completion_window":"24h","status":"validating","created_at":1}`,
			}, http.StatusOK)
			requireDecodes(t, gjson.GetBytes(created, "id").String(), idcodec.KindBatch, "batch_native_2")
			require.Equal(t, endpoint, gjson.GetBytes(created, "endpoint").String())
		})
	}
}
