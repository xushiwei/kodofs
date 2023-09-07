package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/xushiwei/kodofs/internal/kodo/auth"
	"github.com/xushiwei/kodofs/internal/kodo/conf"
	"github.com/xushiwei/kodofs/internal/kodo/reqid"
)

// 用来打印调试信息
var DebugMode = false
var DeepDebugInfo = false

func getUserAgentWithAppName(userApp string) string {
	return fmt.Sprintf("QiniuGo/%s (%s; %s; %s) %s",
		conf.Version, runtime.GOOS, runtime.GOARCH, userApp, runtime.Version())
}

// -----------------------------------------------------------------------------------------

type Client struct {
	*http.Client
}

var UserAgent = getUserAgentWithAppName("default")
var DefaultClient = Client{&http.Client{Transport: http.DefaultTransport}}

func (r Client) CallWithBodyGetter(ctx context.Context, ret interface{}, method, reqUrl string, headers http.Header, body io.Reader,
	getBody func() (io.ReadCloser, error), bodyLength int64) (err error) {

	resp, err := r.DoRequestWithBodyGetter(ctx, method, reqUrl, headers, body, getBody, bodyLength)
	if err != nil {
		return err
	}
	return CallRet(ctx, ret, resp)
}

func (r Client) DoRequestWithBodyGetter(ctx context.Context, method, reqUrl string, headers http.Header, body io.Reader,
	getBody func() (io.ReadCloser, error), bodyLength int64) (resp *http.Response, err error) {

	req, err := newRequest(ctx, method, reqUrl, headers, body)
	if err != nil {
		return
	}
	req.ContentLength = bodyLength
	req.GetBody = getBody
	return r.Do(ctx, req)
}

func (r Client) Do(ctx context.Context, req *http.Request) (resp *http.Response, err error) {
	reqctx := req.Context()

	if reqId, ok := reqid.ReqidFromContext(ctx); ok {
		req.Header.Set("X-Reqid", reqId)
	} else if reqId, ok = reqid.ReqidFromContext(reqctx); ok {
		req.Header.Set("X-Reqid", reqId)
	}

	if _, ok := req.Header["User-Agent"]; !ok {
		req.Header.Set("User-Agent", UserAgent)
	}

	resp, err = r.Client.Do(req)
	return
}

func (r Client) DoRequestWith(ctx context.Context, method, reqUrl string, headers http.Header, body io.Reader,
	bodyLength int) (resp *http.Response, err error) {

	req, err := newRequest(ctx, method, reqUrl, headers, body)
	if err != nil {
		return
	}
	req.ContentLength = int64(bodyLength)
	return r.Do(ctx, req)
}

func (r Client) Call(ctx context.Context, ret interface{}, method, reqUrl string, headers http.Header) (err error) {

	resp, err := r.DoRequestWith(ctx, method, reqUrl, headers, nil, 0)
	if err != nil {
		return err
	}
	return CallRet(ctx, ret, resp)
}

func (r Client) CredentialedCall(ctx context.Context, cred *auth.Credentials, tokenType auth.TokenType, ret interface{},
	method, reqUrl string, headers http.Header) error {
	ctx = auth.WithCredentialsType(ctx, cred, tokenType)
	return r.Call(ctx, ret, method, reqUrl, headers)
}

// --------------------------------------------------------------------

type ErrorInfo struct {
	Err   string `json:"error,omitempty"`
	Key   string `json:"key,omitempty"`
	Reqid string `json:"reqid,omitempty"`
	Errno int    `json:"errno,omitempty"`
	Code  int    `json:"code"`
}

func (r *ErrorInfo) ErrorDetail() string {

	msg, _ := json.Marshal(r)
	return string(msg)
}

func (r *ErrorInfo) Error() string {

	return r.Err
}

func (r *ErrorInfo) RpcError() (code, errno int, key, err string) {

	return r.Code, r.Errno, r.Key, r.Err
}

func (r *ErrorInfo) HttpCode() int {

	return r.Code
}

func parseError(e *ErrorInfo, r io.Reader) {

	body, err1 := io.ReadAll(r)
	if err1 != nil {
		e.Err = err1.Error()
		return
	}

	var ret struct {
		Err   string `json:"error"`
		Key   string `json:"key"`
		Errno int    `json:"errno"`
	}
	if decodeJsonFromData(body, &ret) == nil && ret.Err != "" {
		// qiniu error msg style returns here
		e.Err, e.Key, e.Errno = ret.Err, ret.Key, ret.Errno
		return
	}
	e.Err = string(body)
}

func ResponseError(resp *http.Response) (err error) {

	e := &ErrorInfo{
		Reqid: resp.Header.Get("X-Reqid"),
		Code:  resp.StatusCode,
	}
	if resp.StatusCode > 299 {
		if resp.ContentLength != 0 {
			ct, ok := resp.Header["Content-Type"]
			if ok && strings.HasPrefix(ct[0], "application/json") {
				parseError(e, resp.Body)
			} else {
				bs, rErr := io.ReadAll(resp.Body)
				if rErr != nil {
					err = rErr
				}
				e.Err = strings.TrimRight(string(bs), "\n")
			}
		}
	}
	return e
}

func CallRet(ctx context.Context, ret interface{}, resp *http.Response) (err error) {

	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode/100 == 2 {
		if ret != nil && resp.ContentLength != 0 {
			err = DecodeJsonFromReader(resp.Body, ret)
			if err != nil {
				return
			}
		}
		return nil
	}
	return ResponseError(resp)
}

func newRequest(ctx context.Context, method, reqUrl string, headers http.Header, body io.Reader) (req *http.Request, err error) {
	req, err = http.NewRequest(method, reqUrl, body)
	if err != nil {
		return
	}

	if headers == nil {
		headers = http.Header{}
	}

	err = addDefaultHeader(headers)
	if err != nil {
		return
	}

	req.Header = headers
	req = req.WithContext(ctx)

	//check access token
	mac, t, ok := auth.CredentialsFromContext(ctx)
	if ok {
		err = mac.AddToken(t, req)
		if err != nil {
			return
		}
	}
	return
}

// -----------------------------------------------------------------------------------------

type jsonDecodeError struct {
	original error
	data     []byte
}

func (e jsonDecodeError) Error() string { return fmt.Sprintf("%s: %s", e.original.Error(), e.data) }

func (e jsonDecodeError) Unwrap() error { return e.original }

func decodeJsonFromData(data []byte, v interface{}) error {
	err := json.Unmarshal(data, v)
	if err != nil {
		return jsonDecodeError{original: err, data: data}
	}
	return nil
}

func DecodeJsonFromReader(reader io.Reader, v interface{}) error {
	buf := new(bytes.Buffer)
	t := io.TeeReader(reader, buf)
	err := json.NewDecoder(t).Decode(v)
	if err != nil {
		return jsonDecodeError{original: err, data: buf.Bytes()}
	}
	return nil
}

// -----------------------------------------------------------------------------------------

const (
	RequestHeaderKeyXQiniuDate = "X-Qiniu-Date"
)

func addDefaultHeader(headers http.Header) error {
	return addXQiniuDate(headers)
}

func addXQiniuDate(headers http.Header) error {
	if conf.IsDisableQiniuTimestampSignature() {
		return nil
	}

	timeString := time.Now().UTC().Format("20060102T150405Z")
	headers.Set(RequestHeaderKeyXQiniuDate, timeString)
	return nil
}

// -----------------------------------------------------------------------------------------
