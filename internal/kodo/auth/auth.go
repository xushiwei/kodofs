package auth

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"sort"
	"strings"

	"github.com/xushiwei/kodofs/internal/kodo/api"
	"github.com/xushiwei/kodofs/internal/kodo/conf"
)

// 七牛签名算法的类型：
// QBoxToken, QiniuToken, BearToken, QiniuMacToken
type TokenType int

const (
	TokenQiniu TokenType = iota
	TokenQBox
)

const (
	IAMKeyLen    = 33
	IAMKeyPrefix = "IAM-"

	AuthorizationPrefixQiniu = "Qiniu "
	AuthorizationPrefixQBox  = "QBox "
)

// -----------------------------------------------------------------------------------------

// AK/SK可以从 https://portal.qiniu.com/user/key 获取
type Credentials struct {
	AccessKey string
	SecretKey []byte
}

// 构建一个Credentials对象
func New(accessKey, secretKey string) *Credentials {
	return &Credentials{accessKey, []byte(secretKey)}
}

// Sign 对数据进行签名，一般用于私有空间下载用途
func (ath *Credentials) Sign(data []byte) (token string) {
	h := hmac.New(sha1.New, ath.SecretKey)
	h.Write(data)

	sign := base64.URLEncoding.EncodeToString(h.Sum(nil))
	return fmt.Sprintf("%s:%s", ath.AccessKey, sign)
}

// SignWithData 对数据进行签名，一般用于上传凭证的生成用途
func (ath *Credentials) SignWithData(b []byte) (token string) {
	encodedData := base64.URLEncoding.EncodeToString(b)
	sign := ath.Sign([]byte(encodedData))
	return fmt.Sprintf("%s:%s", sign, encodedData)
}

// SignToken 根据t的类型对请求进行签名，并把token加入req中
func (ath *Credentials) AddToken(t TokenType, req *http.Request) error {
	switch t {
	case TokenQiniu:
		token, sErr := ath.SignRequestV2(req)
		if sErr != nil {
			return sErr
		}
		req.Header.Add("Authorization", AuthorizationPrefixQiniu+token)
	default:
		token, err := ath.SignRequest(req)
		if err != nil {
			return err
		}
		req.Header.Add("Authorization", AuthorizationPrefixQBox+token)
	}
	return nil
}

// SignRequest 对数据进行签名，一般用于管理凭证的生成
func (ath *Credentials) SignRequest(req *http.Request) (token string, err error) {
	data, err := collectData(req)
	if err != nil {
		return
	}
	token = ath.Sign(data)
	return
}

// SignRequestV2 对数据进行签名，一般用于高级管理凭证的生成
func (ath *Credentials) SignRequestV2(req *http.Request) (token string, err error) {

	data, err := collectDataV2(req)
	if err != nil {
		return
	}
	token = ath.Sign(data)
	return
}

type (
	xQiniuHeaderItem struct {
		HeaderName  string
		HeaderValue string
	}
	xQiniuHeaders []xQiniuHeaderItem
)

func (headers xQiniuHeaders) Len() int {
	return len(headers)
}

func (headers xQiniuHeaders) Less(i, j int) bool {
	if headers[i].HeaderName < headers[j].HeaderName {
		return true
	} else if headers[i].HeaderName > headers[j].HeaderName {
		return false
	} else {
		return headers[i].HeaderValue < headers[j].HeaderValue
	}
}

func (headers xQiniuHeaders) Swap(i, j int) {
	headers[i], headers[j] = headers[j], headers[i]
}

func collectDataV2(req *http.Request) (data []byte, err error) {
	u := req.URL

	//write method path?query
	s := fmt.Sprintf("%s %s", req.Method, u.Path)
	if u.RawQuery != "" {
		s += "?"
		s += u.RawQuery
	}

	//write host and post
	s += "\nHost: " + req.Host + "\n"

	//write content type
	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/x-www-form-urlencoded"
		req.Header.Set("Content-Type", contentType)
	}
	s += fmt.Sprintf("Content-Type: %s\n", contentType)

	xQiniuHeaders := make(xQiniuHeaders, 0, len(req.Header))
	for headerName := range req.Header {
		if len(headerName) > len("X-Qiniu-") && strings.HasPrefix(headerName, "X-Qiniu-") {
			xQiniuHeaders = append(xQiniuHeaders, xQiniuHeaderItem{
				HeaderName:  textproto.CanonicalMIMEHeaderKey(headerName),
				HeaderValue: req.Header.Get(headerName),
			})
		}
	}

	if len(xQiniuHeaders) > 0 {
		sort.Sort(xQiniuHeaders)
		for _, xQiniuHeader := range xQiniuHeaders {
			s += fmt.Sprintf("%s: %s\n", xQiniuHeader.HeaderName, xQiniuHeader.HeaderValue)
		}
	}
	s += "\n"

	data = []byte(s)
	//write body
	if incBodyV2(req) {
		s2, rErr := api.BytesFromRequest(req)
		if rErr != nil {
			err = rErr
			return
		}
		req.Body = io.NopCloser(bytes.NewReader(s2))
		data = append(data, s2...)
	}
	return
}

func collectData(req *http.Request) (data []byte, err error) {
	u := req.URL
	s := u.Path
	if u.RawQuery != "" {
		s += "?"
		s += u.RawQuery
	}
	s += "\n"

	data = []byte(s)
	if incBody(req) {
		s2, rErr := api.BytesFromRequest(req)
		if rErr != nil {
			err = rErr
			return
		}
		req.Body = io.NopCloser(bytes.NewReader(s2))
		data = append(data, s2...)
	}
	return
}

// 管理凭证生成时，是否同时对request body进行签名
func incBody(req *http.Request) bool {
	return req.Body != nil && req.Header.Get("Content-Type") == conf.CONTENT_TYPE_FORM
}

func incBodyV2(req *http.Request) bool {
	contentType := req.Header.Get("Content-Type")
	return req.Body != nil && (contentType == conf.CONTENT_TYPE_FORM || contentType == conf.CONTENT_TYPE_JSON)
}

// -----------------------------------------------------------------------------------------

// MacContextKey 是用户的密钥信息
// context.Context中的键值不应该使用普通的字符串， 有可能导致命名冲突
type macContextKey struct{}

// tokenTypeKey 是签名算法类型key
type tokenTypeKey struct{}

// WithCredentials 返回一个包含密钥信息的context
func WithCredentials(ctx context.Context, cred *Credentials) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, macContextKey{}, cred)
}

// WithCredentialsType 返回一个context, 保存了密钥信息和token类型
func WithCredentialsType(ctx context.Context, cred *Credentials, t TokenType) context.Context {
	ctx = WithCredentials(ctx, cred)
	return context.WithValue(ctx, tokenTypeKey{}, t)
}

// CredentialsFromContext 从context获取密钥信息
func CredentialsFromContext(ctx context.Context) (cred *Credentials, t TokenType, ok bool) {
	cred, ok = ctx.Value(macContextKey{}).(*Credentials)
	t, yes := ctx.Value(tokenTypeKey{}).(TokenType)
	if !yes {
		t = TokenQBox
	}
	return
}

// -----------------------------------------------------------------------------------------
