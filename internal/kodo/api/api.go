package api

import (
	"io"
	"net/http"
)

// -----------------------------------------------------------------------------------------

// BytesFromRequest 读取http.Request.Body的内容到slice中
func BytesFromRequest(r *http.Request) (b []byte, err error) {
	if r.ContentLength == 0 {
		return
	}
	if r.ContentLength > 0 {
		b = make([]byte, int(r.ContentLength))
		_, err = io.ReadFull(r.Body, b)
		return
	}
	return io.ReadAll(r.Body)
}

// -----------------------------------------------------------------------------------------

// 可以根据Code判断是何种类型错误
type QError struct {
	Code    string
	Message string
}

// Error 继承error接口
func (e *QError) Error() string {
	return e.Code + ": " + e.Message
}

// NewError 返回QError指针
func NewError(code, message string) *QError {
	return &QError{
		Code:    code,
		Message: message,
	}
}

// -----------------------------------------------------------------------------------------
