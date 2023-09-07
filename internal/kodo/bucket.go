package kodo

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/xushiwei/kodofs/internal/kodo/auth"
	clientv1 "github.com/xushiwei/kodofs/internal/kodo/client"
)

// 资源管理相关的默认域名
const (
	DefaultRsHost  = "rs.qiniu.com"
	DefaultRsfHost = "rsf.qiniu.com"
	DefaultAPIHost = "api.qiniu.com"
	DefaultPubHost = "pu.qbox.me:10200"
)

// -----------------------------------------------------------------------------------------

type BucketManagerOptions struct {
	RetryMax int // 单域名重试次数，当前只有 uc 相关的服务有多域名
	// 主备域名冻结时间（默认：600s），当一个域名请求失败（单个域名会被重试 TryTimes 次），会被冻结一段时间，使用备用域名进行重试，在冻结时间内，域名不能被使用，当一个操作中所有域名竣备冻结操作不在进行重试，返回最后一次操作的错误。
	HostFreezeDuration time.Duration
}

// BucketManager 提供了对资源进行管理的操作
type BucketManager struct {
	Client *clientv1.Client
	Mac    *auth.Credentials
	Cfg    *Config
	// options BucketManagerOptions
}

// NewBucketManager 用来构建一个新的资源管理对象
func NewBucketManager(mac *auth.Credentials, cfg *Config) *BucketManager {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.CentralRsHost == "" {
		cfg.CentralRsHost = DefaultRsHost
	}

	return &BucketManager{
		Client: &clientv1.DefaultClient,
		Mac:    mac,
		Cfg:    cfg,
	}
}

// FetchRet 资源抓取的返回值
type FetchRet struct {
	Hash     string `json:"hash"`
	Fsize    int64  `json:"fsize"`
	MimeType string `json:"mimeType"`
	Key      string `json:"key"`
}

// EncodedEntry 生成URL Safe Base64编码的 Entry
func EncodedEntry(bucket, key string) string {
	entry := fmt.Sprintf("%s:%s", bucket, key)
	return base64.URLEncoding.EncodeToString([]byte(entry))
}

// 构建op的方法，非导出的方法无法用在Batch操作中
func uriFetch(resURL, bucket, key string) string {
	return fmt.Sprintf("/fetch/%s/to/%s",
		base64.URLEncoding.EncodeToString([]byte(resURL)), EncodedEntry(bucket, key))
}

// Fetch 根据提供的远程资源链接来抓取一个文件到空间并已指定文件名保存
func (m *BucketManager) Fetch(resURL, bucket, key string) (fetchRet FetchRet, err error) {
	reqHost, rErr := m.IoReqHost(bucket)
	if rErr != nil {
		err = rErr
		return
	}
	reqURL := fmt.Sprintf("%s%s", reqHost, uriFetch(resURL, bucket, key))
	err = m.Client.CredentialedCall(context.Background(), m.Mac, auth.TokenQiniu, &fetchRet, "POST", reqURL, nil)
	return
}

func (m *BucketManager) IoReqHost(bucket string) (reqHost string, err error) {
	var reqErr error

	if m.Cfg.IoHost == "" {
		reqHost, reqErr = m.IovipHost(bucket)
		if reqErr != nil {
			err = reqErr
			return
		}
	} else {
		reqHost = m.Cfg.IoHost
	}
	if !strings.HasPrefix(reqHost, "http") {
		reqHost = endpoint(m.Cfg.UseHTTPS, reqHost)
	}
	return
}

func (m *BucketManager) IovipHost(bucket string) (iovipHost string, err error) {
	zone, err := m.Zone(bucket)
	if err != nil {
		return
	}

	iovipHost = zone.GetIoHost(m.Cfg.UseHTTPS)
	return
}

func (m *BucketManager) RsfReqHost(bucket string) (reqHost string, err error) {
	var reqErr error

	if m.Cfg.RsfHost == "" {
		reqHost, reqErr = m.RsfHost(bucket)
		if reqErr != nil {
			err = reqErr
			return
		}
	} else {
		reqHost = m.Cfg.RsfHost
	}
	if !strings.HasPrefix(reqHost, "http") {
		reqHost = endpoint(m.Cfg.UseHTTPS, reqHost)
	}
	return
}

func (m *BucketManager) RsfHost(bucket string) (rsfHost string, err error) {
	zone, err := m.Zone(bucket)
	if err != nil {
		return
	}

	rsfHost = zone.GetRsfHost(m.Cfg.UseHTTPS)
	return
}

func (m *BucketManager) Zone(bucket string) (z *Zone, err error) {

	if m.Cfg.Zone != nil {
		z = m.Cfg.Zone
		return
	}

	z, err = GetZone(m.Mac.AccessKey, bucket)
	return
}

// -----------------------------------------------------------------------------------------
