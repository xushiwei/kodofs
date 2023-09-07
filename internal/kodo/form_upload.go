package kodo

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xushiwei/kodofs/internal/kodo/api"
	"github.com/xushiwei/kodofs/internal/kodo/auth"
	"github.com/xushiwei/kodofs/internal/kodo/client"
	"github.com/xushiwei/kodofs/internal/kodo/clientv2"
	"github.com/xushiwei/kodofs/internal/kodo/hostprovider"
	"golang.org/x/sync/singleflight"
)

const (
	// 获取下一个分片Reader失败
	ErrNextReader = "ErrNextReader"
	// 超过了最大的重试上传次数
	ErrMaxUpRetry = "ErrMaxUpRetry"
)

type Region struct {
	// 上传入口
	SrcUpHosts []string `json:"src_up,omitempty"`

	// 加速上传入口
	CdnUpHosts []string `json:"cdn_up,omitempty"`

	// 获取文件信息入口
	RsHost string `json:"rs,omitempty"`

	// bucket列举入口
	RsfHost string `json:"rsf,omitempty"`

	ApiHost string `json:"api,omitempty"`

	// 存储io 入口
	IovipHost string `json:"io,omitempty"`

	// 源站下载入口
	IoSrcHost string `json:"io_src,omitempty"`
}

// 获取io host
func (r *Region) GetIoHost(useHttps bool) string {
	return endpoint(useHttps, r.IovipHost)
}

// 获取rsfHost
func (r *Region) GetRsfHost(useHttps bool) string {
	return endpoint(useHttps, r.RsfHost)
}

// -----------------------------------------------------------------------------------------

type Config struct {
	//兼容保留
	Zone *Region //空间所在的存储区域

	Region *Region

	// 如果设置的Host本身是以http://开头的，又设置了该字段为true，那么优先使用该字段，使用https协议
	// 同理如果该字段为false, 但是设置的host以https开头，那么使用http协议通信
	UseHTTPS      bool   //是否使用https域名
	UseCdnDomains bool   //是否使用cdn加速域名
	CentralRsHost string //中心机房的RsHost，用于list bucket

	// 兼容保留
	RsHost  string
	RsfHost string
	UpHost  string
	ApiHost string
	IoHost  string
}

// GetRegion返回一个Region指针
// 默认返回最新的Region， 如果该字段没有，那么返回兼容保留的Zone, 如果都为nil, 就返回nil
func (c *Config) GetRegion() *Region {
	if c.Region != nil {
		return c.Region
	}
	if c.Zone != nil {
		return c.Zone
	}
	return nil
}

// PutRet 为七牛标准的上传回复内容。
// 如果使用了上传回调或者自定义了returnBody，那么需要根据实际情况，自己自定义一个返回值结构体
type PutRet struct {
	Hash         string `json:"hash"`
	PersistentID string `json:"persistentId"`
	Key          string `json:"key"`
}

type FormUploader struct {
	Client *client.Client
	Cfg    *Config
}

func NewFormUploader(cfg *Config) *FormUploader {
	if cfg == nil {
		cfg = &Config{}
	}

	return &FormUploader{
		Client: &client.DefaultClient,
		Cfg:    cfg,
	}
}

func NewFormUploaderEx(cfg *Config, clt *client.Client) *FormUploader {
	if cfg == nil {
		cfg = &Config{}
	}

	if clt == nil {
		clt = &client.DefaultClient
	}

	return &FormUploader{
		Client: clt,
		Cfg:    cfg,
	}
}

// -----------------------------------------------------------------------------------------

type PutExtra struct {
	// 可选，用户自定义参数，必须以 "x:" 开头。若不以x:开头，则忽略。
	Params map[string]string

	UpHost string

	TryTimes int // 可选。尝试次数

	// 主备域名冻结时间（默认：600s），当一个域名请求失败（单个域名会被重试 TryTimes 次），会被冻结一段时间，使用备用域名进行重试，在冻结时间内，域名不能被使用，当一个操作中所有域名竣备冻结操作不在进行重试，返回最后一次操作的错误。
	HostFreezeDuration time.Duration

	// 可选，当为 "" 时候，服务端自动判断。
	MimeType string

	// 上传事件：进度通知。这个事件的回调函数应该尽可能快地结束。
	OnProgress func(fsize, uploaded int64)
}

const (
	defaultTryTimes = 3
)

func (extra *PutExtra) init() {
	if extra.TryTimes == 0 {
		extra.TryTimes = defaultTryTimes
	}
	if extra.HostFreezeDuration <= 0 {
		extra.HostFreezeDuration = 10 * 60 * time.Second
	}
}

func (extra *PutExtra) getUpHost(useHttps bool) string {
	return hostAddSchemeIfNeeded(useHttps, extra.UpHost)
}

func hostAddSchemeIfNeeded(useHttps bool, host string) string {
	if host == "" {
		return ""
	} else if strings.Contains(host, "://") {
		return host
	} else {
		return endpoint(useHttps, host)
	}
}

func endpoint(useHttps bool, host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}

	if strings.HasPrefix(host, "http://") ||
		strings.HasPrefix(host, "https://") {
		return host
	}

	scheme := "http://"
	if useHttps {
		scheme = "https://"
	}
	return fmt.Sprintf("%s%s", scheme, host)
}

// -----------------------------------------------------------------------------------------

type PutPolicy struct {

	// 指定上传的目标资源空间 Bucket 和资源键 Key（最大为 750 字节）。有三种格式：
	// <bucket>，表示允许用户上传文件到指定的 bucket。在这种格式下文件只能新增（分片上传 v1 版 需要指定 insertOnly 为 1 才是新增，否则也为覆盖上传），若已存在同名资源（且文件内容/etag不一致），上传会失败；若已存在资源的内容/etag一致，则上传会返回成功。
	// <bucket>:<key>，表示只允许用户上传指定 key 的文件。在这种格式下文件默认允许修改，若已存在同名资源则会被覆盖。如果只希望上传指定 key 的文件，并且不允许修改，那么可以将下面的 insertOnly 属性值设为 1。
	// <bucket>:<keyPrefix>，表示只允许用户上传指定以 keyPrefix 为前缀的文件，当且仅当 isPrefixalScope 字段为 1 时生效，isPrefixalScope 为 1 时无法覆盖上传。
	Scope string `json:"scope"`

	// 若为 1，表示允许用户上传以 scope 的 keyPrefix 为前缀的文件。
	IsPrefixalScope int `json:"isPrefixalScope,omitempty"`

	// 上传凭证有效截止时间。Unix时间戳，单位为秒。该截止时间为上传完成后，在七牛空间生成文件的校验时间，而非上传的开始时间，
	// 一般建议设置为上传开始时间 + 3600s，用户可根据具体的业务场景对凭证截止时间进行调整。
	Expires uint64 `json:"deadline"`

	// 若非0, 即使Scope为 Bucket:Key 的形式也是insert only
	InsertOnly uint16 `json:"insertOnly,omitempty"`

	// 唯一属主标识。特殊场景下非常有用，例如根据 App-Client 标识给图片或视频打水印。
	EndUser string `json:"endUser,omitempty"`

	// Web 端文件上传成功后，浏览器执行 303 跳转的 URL。通常用于表单上传。
	// 文件上传成功后会跳转到 <returnUrl>?upload_ret=<queryString>，<queryString>包含 returnBody 内容。
	// 如不设置 returnUrl，则直接将 returnBody 的内容返回给客户端。
	ReturnURL string `json:"returnUrl,omitempty"`

	// 上传成功后，自定义七牛云最终返回給上传端（在指定 returnUrl 时是携带在跳转路径参数中）的数据。支持魔法变量和自定义变量。
	// returnBody 要求是合法的 JSON 文本。
	// 例如 {“key”: $(key), “hash”: $(etag), “w”: $(imageInfo.width), “h”: $(imageInfo.height)}。
	ReturnBody string `json:"returnBody,omitempty"`

	// 上传成功后，七牛云向业务服务器发送 POST 请求的 URL。必须是公网上可以正常进行 POST 请求并能响应 HTTP/1.1 200 OK 的有效 URL。
	// 另外，为了给客户端有一致的体验，我们要求 callbackUrl 返回包 Content-Type 为 “application/json”，即返回的内容必须是合法的
	// JSON 文本。出于高可用的考虑，本字段允许设置多个 callbackUrl（用英文符号 ; 分隔），在前一个 callbackUrl 请求失败的时候会依次
	// 重试下一个 callbackUrl。一个典型例子是：http://<ip1>/callback;http://<ip2>/callback，并同时指定下面的 callbackHost 字段。
	// 在 callbackUrl 中使用 ip 的好处是减少对 dns 解析的依赖，可改善回调的性能和稳定性。指定 callbackUrl，必须指定 callbackbody，
	// 且值不能为空。
	CallbackURL string `json:"callbackUrl,omitempty"`

	// 上传成功后，七牛云向业务服务器发送回调通知时的 Host 值。与 callbackUrl 配合使用，仅当设置了 callbackUrl 时才有效。
	CallbackHost string `json:"callbackHost,omitempty"`

	// 上传成功后，七牛云向业务服务器发送 Content-Type: application/x-www-form-urlencoded 的 POST 请求。业务服务器可以通过直接读取
	// 请求的 query 来获得该字段，支持魔法变量和自定义变量。callbackBody 要求是合法的 url query string。
	// 例如key=$(key)&hash=$(etag)&w=$(imageInfo.width)&h=$(imageInfo.height)。如果callbackBodyType指定为application/json，
	// 则callbackBody应为json格式，例如:{“key”:"$(key)",“hash”:"$(etag)",“w”:"$(imageInfo.width)",“h”:"$(imageInfo.height)"}。
	CallbackBody string `json:"callbackBody,omitempty"`

	// 上传成功后，七牛云向业务服务器发送回调通知 callbackBody 的 Content-Type。默认为 application/x-www-form-urlencoded，也可设置
	// 为 application/json。
	CallbackBodyType string `json:"callbackBodyType,omitempty"`

	// 资源上传成功后触发执行的预转持久化处理指令列表。fileType=2或3（上传归档存储或深度归档存储文件）时，不支持使用该参数。支持魔法变量和自
	// 定义变量。每个指令是一个 API 规格字符串，多个指令用;分隔。请参阅persistenOps详解与示例。同时添加 persistentPipeline 字段，使用专
	// 用队列处理，请参阅persistentPipeline。
	PersistentOps string `json:"persistentOps,omitempty"`

	// 接收持久化处理结果通知的 URL。必须是公网上可以正常进行 POST 请求并能响应 HTTP/1.1 200 OK 的有效 URL。该 URL 获取的内容和持久化处
	// 理状态查询的处理结果一致。发送 body 格式是 Content-Type 为 application/json 的 POST 请求，需要按照读取流的形式读取请求的 body
	// 才能获取。
	PersistentNotifyURL string `json:"persistentNotifyUrl,omitempty"`

	// 转码队列名。资源上传成功后，触发转码时指定独立的队列进行转码。为空则表示使用公用队列，处理速度比较慢。建议使用专用队列。
	PersistentPipeline string `json:"persistentPipeline,omitempty"`

	// saveKey 的优先级设置。为 true 时，saveKey不能为空，会忽略客户端指定的key，强制使用saveKey进行文件命名。参数不设置时，
	// 默认值为false
	ForceSaveKey bool `json:"forceSaveKey,omitempty"` //

	// 自定义资源名。支持魔法变量和自定义变量。forceSaveKey 为false时，这个字段仅当用户上传的时候没有主动指定 key 时起作用；
	// forceSaveKey 为true时，将强制按这个字段的格式命名。
	SaveKey string `json:"saveKey,omitempty"`

	// 限定上传文件大小最小值，单位Byte。小于限制上传文件大小的最小值会被判为上传失败，返回 403 状态码
	FsizeMin int64 `json:"fsizeMin,omitempty"`

	// 限定上传文件大小最大值，单位Byte。超过限制上传文件大小的最大值会被判为上传失败，返回 413 状态码。
	FsizeLimit int64 `json:"fsizeLimit,omitempty"`

	// 开启 MimeType 侦测功能，并按照下述规则进行侦测；如不能侦测出正确的值，会默认使用 application/octet-stream 。
	// 设为非 0 值，则忽略上传端传递的文件 MimeType 信息，并按如下顺序侦测 MimeType 值：
	// 1. 侦测内容； 2. 检查文件扩展名； 3. 检查 Key 扩展名。
	// 默认设为 0 值，如上传端指定了 MimeType 则直接使用该值，否则按如下顺序侦测 MimeType 值：
	// 1. 检查文件扩展名； 2. 检查 Key 扩展名； 3. 侦测内容。
	DetectMime uint8 `json:"detectMime,omitempty"`

	// 限定用户上传的文件类型。指定本字段值，七牛服务器会侦测文件内容以判断 MimeType，再用判断值跟指定值进行匹配，匹配成功则允许上传，匹配失败则返回 403 状态码。示例：
	// image/* 表示只允许上传图片类型
	// image/jpeg;image/png 表示只允许上传 jpg 和 png 类型的图片
	// !application/json;text/plain 表示禁止上传 json 文本和纯文本。注意最前面的感叹号！
	MimeLimit string `json:"mimeLimit,omitempty"`

	// 资源的存储类型，0表示标准存储，1 表示低频存储，2 表示归档存储，3 表示深度归档存储。
	FileType int `json:"fileType,omitempty"`

	CallbackFetchKey uint8 `json:"callbackFetchKey,omitempty"`

	DeleteAfterDays int `json:"deleteAfterDays,omitempty"`
}

// UploadToken 方法用来进行上传凭证的生成
// 该方法生成的过期时间是现对于现在的时间
func (p *PutPolicy) UploadToken(cred *auth.Credentials) string {
	return p.uploadToken(cred)
}

func (p PutPolicy) uploadToken(cred *auth.Credentials) (token string) {
	if p.Expires == 0 {
		p.Expires = 3600 // 默认一小时过期
	}
	p.Expires += uint64(time.Now().Unix())
	putPolicyJSON, _ := json.Marshal(p)
	token = cred.SignWithData(putPolicyJSON)
	return
}

// -----------------------------------------------------------------------------------------

// Put 用来以表单方式上传一个文件。
//
// ctx     是请求的上下文。
// ret     是上传成功后返回的数据。如果 uptoken 中没有设置 callbackUrl 或 returnBody，那么返回的数据结构是 PutRet 结构。
// uptoken 是由业务服务器颁发的上传凭证。
// key     是要上传的文件访问路径。比如："foo/bar.jpg"。注意我们建议 key 不要以 '/' 开头。另外，key 为空字符串是合法的。
// data    是文件内容的访问接口（io.Reader）。
// fsize   是要上传的文件大小。
// extra   是上传的一些可选项。可以指定为nil。详细见 PutExtra 结构的描述。
func (p *FormUploader) Put(
	ctx context.Context, ret interface{}, uptoken, key string, data io.Reader, size int64, extra *PutExtra) (err error) {
	err = p.put(ctx, ret, uptoken, key, true, data, size, extra, path.Base(key))
	return
}

func (p *FormUploader) PutFile(
	ctx context.Context, ret interface{}, uptoken, key, localFile string, extra *PutExtra) (err error) {
	return p.putFile(ctx, ret, uptoken, key, true, localFile, extra)
}

func (p *FormUploader) putFile(
	ctx context.Context, ret interface{}, upToken string,
	key string, hasKey bool, localFile string, extra *PutExtra) (err error) {

	f, err := os.Open(localFile)
	if err != nil {
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return
	}
	fsize := fi.Size()

	return p.put(ctx, ret, upToken, key, hasKey, f, fsize, extra, filepath.Base(localFile))
}

func (p *FormUploader) put(
	ctx context.Context, ret interface{}, upToken string,
	key string, hasKey bool, data io.Reader, size int64, extra *PutExtra, fileName string) error {

	if extra == nil {
		extra = &PutExtra{}
	}
	extra.init()

	seekableData, ok := data.(io.ReadSeeker)
	if !ok {
		dataBytes, rErr := io.ReadAll(data)
		if rErr != nil {
			return rErr
		}
		if size <= 0 {
			size = int64(len(dataBytes))
		}
		seekableData = bytes.NewReader(dataBytes)
	}

	return p.putSeekableData(ctx, ret, upToken, key, hasKey, seekableData, size, extra, fileName)
}

func (p *FormUploader) putSeekableData(ctx context.Context, ret interface{}, upToken string,
	key string, hasKey bool, data io.ReadSeeker, dataSize int64, extra *PutExtra, fileName string) error {

	formFieldBuff := new(bytes.Buffer)
	formWriter := multipart.NewWriter(formFieldBuff)
	// 写入表单头、token、key、fileName 等信息
	if wErr := writeMultipart(formWriter, upToken, key, hasKey, extra, fileName); wErr != nil {
		return wErr
	}

	// 计算文件 crc32
	crc32Hash := crc32.NewIEEE()
	if _, cErr := io.Copy(crc32Hash, data); cErr != nil {
		return cErr
	}
	crcReader := newCrc32Reader(formWriter.Boundary(), crc32Hash)
	crcBytes, rErr := io.ReadAll(crcReader)
	if rErr != nil {
		return rErr
	}
	crcReader = nil

	// 表单写入文件 crc32
	if _, wErr := formFieldBuff.Write(crcBytes); wErr != nil {
		return wErr
	}
	crcBytes = nil

	formHead := make(textproto.MIMEHeader)
	formHead.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`,
		escapeQuotes(fileName)))
	if extra.MimeType != "" {
		formHead.Set("Content-Type", extra.MimeType)
	}
	if _, cErr := formWriter.CreatePart(formHead); cErr != nil {
		return cErr
	}
	formHead = nil

	// 表单 Fields
	formFieldData := formFieldBuff.Bytes()
	formFieldBuff = nil

	// 表单最后一行
	formEndLine := []byte(fmt.Sprintf("\r\n--%s--\r\n", formWriter.Boundary()))

	// 不再重新构造 formBody ，避免内存峰值问题
	var formBodyLen int64 = -1
	if dataSize >= 0 {
		formBodyLen = int64(len(formFieldData)) + dataSize + int64(len(formEndLine))
	}

	progress := newUploadProgress(extra.OnProgress)
	getBodyReader := func() (io.Reader, error) {
		if _, err := data.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}

		var formReader = io.MultiReader(bytes.NewReader(formFieldData), data, bytes.NewReader(formEndLine))
		if extra.OnProgress != nil {
			formReader = &readerWithProgress{reader: formReader, fsize: formBodyLen, onProgress: progress.onProgress}
		}
		return formReader, nil
	}
	getBodyReadCloser := func() (io.ReadCloser, error) {
		reader, err := getBodyReader()
		if err != nil {
			return nil, err
		}
		return io.NopCloser(reader), nil
	}

	var err error
	var hostProvider hostprovider.HostProvider = nil
	if extra.UpHost != "" {
		hostProvider = hostprovider.NewWithHosts([]string{extra.getUpHost(p.Cfg.UseHTTPS)})
	} else {
		hostProvider, err = p.getUpHostProviderFromUploadToken(upToken, extra)
		if err != nil {
			return err
		}
	}

	// 上传
	contentType := formWriter.FormDataContentType()
	headers := http.Header{}
	headers.Add("Content-Type", contentType)
	err = doUploadAction(hostProvider, extra.TryTimes, extra.HostFreezeDuration, func(host string) error {
		reader, gErr := getBodyReader()
		if gErr != nil {
			return gErr
		}

		return p.Client.CallWithBodyGetter(ctx, ret, "POST", host, headers, reader, getBodyReadCloser, formBodyLen)
	})
	if err != nil {
		return err
	}
	if extra.OnProgress != nil {
		extra.OnProgress(formBodyLen, formBodyLen)
	}
	return nil
}

func (p *FormUploader) getUpHostProviderFromUploadToken(upToken string, extra *PutExtra) (hostprovider.HostProvider, error) {
	ak, bucket, err := getAkBucketFromUploadToken(upToken)
	if err != nil {
		return nil, err
	}
	return getUpHostProvider(p.Cfg, extra.TryTimes, extra.HostFreezeDuration, ak, bucket)
}

// retryMax: 为 0，使用默认值，每个域名只请求一次
// hostFreezeDuration: 为 0，使用默认值：50ms ~ 100ms
func getUpHostProvider(config *Config, retryMax int, hostFreezeDuration time.Duration, ak, bucket string) (hostprovider.HostProvider, error) {
	region := config.GetRegion()
	var err error
	if region == nil {
		if region, err = GetRegionWithOptions(ak, bucket, UCApiOptions{
			RetryMax:           retryMax,
			HostFreezeDuration: hostFreezeDuration,
		}); err != nil {
			return nil, err
		}
	}

	hosts := make([]string, 0, 4)
	if config.UseCdnDomains && len(region.CdnUpHosts) > 0 {
		hosts = append(hosts, region.CdnUpHosts...)
	} else if len(region.SrcUpHosts) > 0 {
		hosts = append(hosts, region.SrcUpHosts...)
	}

	for i := 0; i < len(hosts); i++ {
		hosts[i] = endpoint(config.UseHTTPS, hosts[i])
	}

	return hostprovider.NewWithHosts(hosts), nil
}

func getAkBucketFromUploadToken(token string) (ak, bucket string, err error) {
	items := strings.Split(token, ":")
	// KODO-11919
	if len(items) == 5 && items[0] == "" {
		items = items[2:]
	} else if len(items) != 3 {
		err = errors.New("invalid upload token, format error")
		return
	}

	ak = items[0]
	policyBytes, dErr := base64.URLEncoding.DecodeString(items[2])
	if dErr != nil {
		err = errors.New("invalid upload token, invalid put policy")
		return
	}

	putPolicy := PutPolicy{}
	uErr := json.Unmarshal(policyBytes, &putPolicy)
	if uErr != nil {
		err = errors.New("invalid upload token, invalid put policy")
		return
	}

	bucket = strings.Split(putPolicy.Scope, ":")[0]
	return
}

type UCApiOptions struct {
	UseHttps bool //
	RetryMax int  // 单域名重试次数
	// 主备域名冻结时间（默认：600s），当一个域名请求失败（单个域名会被重试 TryTimes 次），会被冻结一段时间，使用备用域名进行重试，在冻结时间内，域名不能被使用，当一个操作中所有域名竣备冻结操作不在进行重试，返回最后一次操作的错误。
	HostFreezeDuration time.Duration
}

// 此处废弃，但为了兼容老版本，单独放置一个文件

// UcQueryServerInfo 为查询请求回复中的上传域名信息
type UcQueryServerInfo struct {
	Main   []string `json:"main,omitempty"`
	Backup []string `json:"backup,omitempty"`
	Info   string   `json:"info,omitempty"`
}

type UcQueryUp = UcQueryServerInfo
type UcQueryIo = UcQueryServerInfo

// UcQueryRet 为查询请求的回复
type UcQueryRet struct {
	TTL       int                            `json:"ttl"`
	Io        map[string]map[string][]string `json:"-"`
	IoInfo    map[string]UcQueryIo           `json:"io"`
	IoSrcInfo map[string]UcQueryIo           `json:"io_src"`
	Up        map[string]UcQueryUp           `json:"up"`
	RsInfo    map[string]UcQueryServerInfo   `json:"rs"`
	RsfInfo   map[string]UcQueryServerInfo   `json:"rsf"`
	ApiInfo   map[string]UcQueryServerInfo   `json:"api"`
}

func (uc *UcQueryRet) getOneHostFromInfo(info map[string]UcQueryIo) string {
	if len(info["src"].Main) > 0 {
		return info["src"].Main[0]
	}

	if len(info["acc"].Main) > 0 {
		return info["acc"].Main[0]
	}

	return ""
}

var ucQueryV2Group singleflight.Group

type regionV2CacheValue struct {
	Region   *Region   `json:"region"`
	Deadline time.Time `json:"deadline"`
}

type regionV2CacheMap map[string]regionV2CacheValue

const regionV2CacheFileName = "query_v2_00.cache.json"

var (
	regionV2CachePath     = filepath.Join(os.TempDir(), "qiniu-golang-sdk", regionV2CacheFileName)
	regionV2Cache         sync.Map
	regionV2CacheLock     sync.RWMutex
	regionV2CacheSyncLock sync.Mutex
	regionV2CacheLoaded   bool = false
)

func loadRegionV2Cache() {
	cacheFile, err := os.Open(regionV2CachePath)
	if err != nil {
		return
	}
	defer cacheFile.Close()

	var cacheMap regionV2CacheMap
	if err = json.NewDecoder(cacheFile).Decode(&cacheMap); err != nil {
		return
	}
	for cacheKey, cacheValue := range cacheMap {
		regionV2Cache.Store(cacheKey, cacheValue)
	}
}

func storeRegionV2Cache() {
	err := os.MkdirAll(filepath.Dir(regionV2CachePath), 0700)
	if err != nil {
		return
	}

	cacheFile, err := os.OpenFile(regionV2CachePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return
	}
	defer cacheFile.Close()

	cacheMap := make(regionV2CacheMap)
	regionV2Cache.Range(func(cacheKey, cacheValue interface{}) bool {
		cacheMap[cacheKey.(string)] = cacheValue.(regionV2CacheValue)
		return true
	})
	if err = json.NewEncoder(cacheFile).Encode(cacheMap); err != nil {
		return
	}
}

const (
	defaultApiHost = "api.qiniu.com"
	defaultUcHost0 = "kodo-config.qiniuapi.com"
	defaultUcHost1 = "uc.qbox.me"
)

// UcHost 为查询空间相关域名的 API 服务地址
// 设置 UcHost 时，如果不指定 scheme 默认会使用 https
// Deprecated 使用 SetUcHosts 替换
var UcHost = ""

// 公有云包括 defaultApiHost，非 uc query api 使用时需要移除 defaultApiHost
// 用户配置时，不能配置 api 域名
var ucHosts = []string{defaultUcHost0, defaultUcHost1, defaultApiHost}

func getUcHost(useHttps bool) string {
	// 兼容老版本，优先使用 UcHost
	host := ""
	if len(UcHost) > 0 {
		host = UcHost
	} else if len(ucHosts) > 0 {
		host = ucHosts[0]
	}
	return endpoint(useHttps, host)
}

type ucClientConfig struct {
	// 非 uc query api 需要去除默认域名 defaultApiHost
	IsUcQueryApi bool

	// 单域名重试次数
	RetryMax int

	// 主备域名冻结时间（默认：600s），当一个域名请求失败（单个域名会被重试 TryTimes 次），会被冻结一段时间，使用备用域名进行重试，在冻结时间内，域名不能被使用，当一个操作中所有域名竣备冻结操作不在进行重试，返回最后一次操作的错误。
	HostFreezeDuration time.Duration

	Client *client.Client
}

// 不带 scheme
func getUcBackupHosts() []string {
	var hosts []string
	if len(UcHost) > 0 {
		hosts = append(hosts, removeHostScheme(UcHost))
	}

	for _, host := range ucHosts {
		if len(host) > 0 {
			hosts = append(hosts, removeHostScheme(host))
		}
	}

	hosts = removeRepeatStringItem(hosts)
	return hosts
}

func removeRepeatStringItem(slc []string) []string {
	var result []string
	tempMap := map[string]uint8{}
	for _, e := range slc {
		l := len(tempMap)
		tempMap[e] = 0
		if len(tempMap) != l {
			result = append(result, e)
		}
	}
	return result
}

func removeHostScheme(host string) string {
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "https://")
	return host
}

func getUCClient(config ucClientConfig, mac *auth.Credentials) clientv2.Client {
	allHosts := getUcBackupHosts()
	var hosts []string = nil
	if !config.IsUcQueryApi {
		// 非 uc query api 去除 defaultApiHost
		for _, host := range allHosts {
			if host != defaultApiHost {
				hosts = append(hosts, host)
			}
		}
	} else {
		hosts = allHosts
	}

	is := []clientv2.Interceptor{
		clientv2.NewHostsRetryInterceptor(clientv2.HostsRetryConfig{
			RetryConfig: clientv2.RetryConfig{
				RetryMax:      len(hosts),
				RetryInterval: nil,
				ShouldRetry:   nil,
			},
			ShouldFreezeHost:   nil,
			HostFreezeDuration: 0,
			HostProvider:       hostprovider.NewWithHosts(hosts),
		}),
		clientv2.NewSimpleRetryInterceptor(clientv2.RetryConfig{
			RetryMax:      config.RetryMax,
			RetryInterval: nil,
			ShouldRetry:   nil,
		}),
	}

	if mac != nil {
		is = append(is, clientv2.NewAuthInterceptor(clientv2.AuthConfig{
			Credentials: *mac,
			TokenType:   auth.TokenQiniu,
		}))
	}

	return clientv2.NewClient(clientv2.NewClientWithClientV1(config.Client), is...)
}

func getRegionByV2(ak, bucket string, options UCApiOptions) (*Region, error) {

	regionV2CacheLock.RLock()
	if regionV2CacheLoaded {
		regionV2CacheLock.RUnlock()
	} else {
		regionV2CacheLock.RUnlock()
		func() {
			regionV2CacheLock.Lock()
			defer regionV2CacheLock.Unlock()

			if !regionV2CacheLoaded {
				loadRegionV2Cache()
				regionV2CacheLoaded = true
			}
		}()
	}

	regionID := fmt.Sprintf("%s:%s", ak, bucket)
	//check from cache
	if v, ok := regionV2Cache.Load(regionID); ok && time.Now().Before(v.(regionV2CacheValue).Deadline) {
		return v.(regionV2CacheValue).Region, nil
	}

	newRegion, err, _ := ucQueryV2Group.Do(regionID, func() (interface{}, error) {
		reqURL := fmt.Sprintf("%s/v2/query?ak=%s&bucket=%s", getUcHost(options.UseHttps), ak, bucket)

		var ret UcQueryRet
		c := getUCClient(ucClientConfig{
			IsUcQueryApi:       true,
			RetryMax:           options.RetryMax,
			HostFreezeDuration: options.HostFreezeDuration,
		}, nil)
		_, err := clientv2.DoAndDecodeJsonResponse(c, clientv2.RequestParams{
			Context:     context.Background(),
			Method:      clientv2.RequestMethodGet,
			Url:         reqURL,
			Header:      nil,
			BodyCreator: nil,
		}, &ret)
		if err != nil {
			return nil, fmt.Errorf("query region error, %s", err.Error())
		}

		ioHost := ret.getOneHostFromInfo(ret.IoInfo)
		if len(ioHost) == 0 {
			return nil, fmt.Errorf("empty io host list")
		}

		ioSrcHost := ret.getOneHostFromInfo(ret.IoSrcInfo)
		if len(ioHost) == 0 {
			return nil, fmt.Errorf("empty io host list")
		}

		rsHost := ret.getOneHostFromInfo(ret.RsInfo)
		if len(rsHost) == 0 {
			return nil, fmt.Errorf("empty rs host list")
		}

		rsfHost := ret.getOneHostFromInfo(ret.RsfInfo)
		if len(rsfHost) == 0 {
			return nil, fmt.Errorf("empty rsf host list")
		}

		apiHost := ret.getOneHostFromInfo(ret.ApiInfo)
		if len(apiHost) == 0 {
			return nil, fmt.Errorf("empty api host list")
		}

		srcUpHosts := ret.Up["src"].Main
		if ret.Up["src"].Backup != nil {
			srcUpHosts = append(srcUpHosts, ret.Up["src"].Backup...)
		}
		cdnUpHosts := ret.Up["acc"].Main
		if ret.Up["acc"].Backup != nil {
			cdnUpHosts = append(cdnUpHosts, ret.Up["acc"].Backup...)
		}

		region := &Region{
			SrcUpHosts: srcUpHosts,
			CdnUpHosts: cdnUpHosts,
			IovipHost:  ioHost,
			RsHost:     rsHost,
			RsfHost:    rsfHost,
			ApiHost:    apiHost,
			IoSrcHost:  ioSrcHost,
		}

		regionV2Cache.Store(regionID, regionV2CacheValue{
			Region:   region,
			Deadline: time.Now().Add(time.Duration(ret.TTL) * time.Second),
		})

		regionV2CacheSyncLock.Lock()
		defer regionV2CacheSyncLock.Unlock()

		storeRegionV2Cache()
		return region, nil
	})
	if newRegion == nil {
		return nil, err
	}

	return newRegion.(*Region), err
}

// Zone 是Region的别名
// 兼容保留
type Zone = Region

// GetZone 用来根据ak和bucket来获取空间相关的机房信息
// 新版本使用GetRegion, 这个函数用来保持兼容
func GetZone(ak, bucket string) (zone *Zone, err error) {
	return GetRegion(ak, bucket)
}

func DefaultUCApiOptions() UCApiOptions {
	return UCApiOptions{
		UseHttps:           true,
		RetryMax:           0,
		HostFreezeDuration: 0,
	}
}

// GetRegion 用来根据ak和bucket来获取空间相关的机房信息
// 延用 v2, v2 结构和 v4 结构不同且暂不可替代
// Deprecated 使用 GetRegionWithOptions 替换
func GetRegion(ak, bucket string) (*Region, error) {
	return GetRegionWithOptions(ak, bucket, DefaultUCApiOptions())
}

// GetRegionWithOptions 用来根据ak和bucket来获取空间相关的机房信息
func GetRegionWithOptions(ak, bucket string, options UCApiOptions) (*Region, error) {
	return getRegionByV2(ak, bucket, options)
}

func shouldUploadRetryWithOtherHost(err error) bool {
	return clientv2.IsErrorRetryable(err)
}

func doUploadAction(hostProvider hostprovider.HostProvider, retryMax int, freezeDuration time.Duration, action func(host string) error) error {
	for {
		host, err := hostProvider.Provider()
		if err != nil {
			return api.NewError(ErrMaxUpRetry, err.Error())
		}

		for i := 0; ; i++ {
			err = action(host)

			// 请求成功
			if err == nil {
				return nil
			}

			// 不可重试错误
			if !shouldUploadRetryWithOtherHost(err) {
				return err
			}

			// 超过重试次数退出
			if i >= retryMax {
				break
			}
		}

		// 单个 host 失败，冻结此 host，换其他 host
		_ = hostProvider.Freeze(host, err, freezeDuration)
	}
}

var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

func escapeQuotes(s string) string {
	return quoteEscaper.Replace(s)
}

func writeMultipart(writer *multipart.Writer, uptoken, key string, hasKey bool,
	extra *PutExtra, fileName string) (err error) {

	//token
	if err = writer.WriteField("token", uptoken); err != nil {
		return
	}

	//key
	if hasKey {
		if err = writer.WriteField("key", key); err != nil {
			return
		}
	}

	//extra.Params
	if extra.Params != nil {
		for k, v := range extra.Params {
			if (strings.HasPrefix(k, "x:") || strings.HasPrefix(k, "x-qn-meta-")) && v != "" {
				err = writer.WriteField(k, v)
				if err != nil {
					return
				}
			}
		}
	}

	return err
}

// -----------------------------------------------------------------------------------------

type crc32Reader struct {
	h                hash.Hash32
	boundary         string
	r                io.Reader
	inited           bool
	nlDashBoundaryNl string
	header           string
	crc32PadLen      int64
}

func newCrc32Reader(boundary string, h hash.Hash32) *crc32Reader {
	nlDashBoundaryNl := fmt.Sprintf("\r\n--%s\r\n", boundary)
	header := `Content-Disposition: form-data; name="crc32"` + "\r\n\r\n"
	return &crc32Reader{
		h:                h,
		boundary:         boundary,
		nlDashBoundaryNl: nlDashBoundaryNl,
		header:           header,
		crc32PadLen:      10,
	}
}

func (r *crc32Reader) Read(p []byte) (int, error) {
	if !r.inited {
		crc32Sum := r.h.Sum32()
		crc32Line := r.nlDashBoundaryNl + r.header + fmt.Sprintf("%010d", crc32Sum) //padding crc32 results to 10 digits
		r.r = strings.NewReader(crc32Line)
		r.inited = true
	}
	return r.r.Read(p)
}

/*
func (r crc32Reader) length() (length int64) {
	return int64(len(r.nlDashBoundaryNl+r.header)) + r.crc32PadLen
}
*/

// -----------------------------------------------------------------------------------------

type uploadProgress struct {
	lastUploadedBytes int64
	progress          func(totalBytes, uploadedBytes int64)
}

func newUploadProgress(progressHandler func(totalBytes, uploadedBytes int64)) *uploadProgress {
	return &uploadProgress{
		lastUploadedBytes: 0,
		progress:          progressHandler,
	}
}

func (p *uploadProgress) onProgress(totalBytes, uploadedBytes int64) {
	if p.progress == nil {
		return
	}

	if p.lastUploadedBytes >= uploadedBytes {
		// 过滤重新上传的场景
		return
	}
	p.lastUploadedBytes = uploadedBytes
	p.progress(totalBytes, uploadedBytes)
}

type readerWithProgress struct {
	reader     io.Reader
	uploaded   int64
	fsize      int64
	onProgress func(fsize, uploaded int64)
}

func (p *readerWithProgress) Read(b []byte) (n int, err error) {
	if p.uploaded > 0 {
		p.onProgress(p.fsize, p.uploaded)
	}

	n, err = p.reader.Read(b)
	p.uploaded += int64(n)
	if p.fsize > 0 && p.uploaded > p.fsize {
		p.uploaded = p.fsize
	}
	return
}

// -----------------------------------------------------------------------------------------
