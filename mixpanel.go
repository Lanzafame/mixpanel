package mixpanel

// http://play.golang.org/p/vd8qr3TGRz

import (
	"crypto/md5"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"time"

	"utils"
)

const (
	// Endpoint const
	Endpoint string = "http://mixpanel.com/api"
	// RawEndpoint const
	RawEndpoint string = "http://data.mixpanel.com/api"
	// Version const
	Version string = "2.0"
	// Format const
	Format string = "json"
)

// Request object
type Request struct {
	Endpoint   string
	Method     string
	Parameters map[string]string
	Expire     string
	Signature  string
	Config     Config
}

// Config ...
type Config struct {
	APIKey    string
	APISecret string
}

// ConfigureAuth ...
func (req *Request) ConfigureAuth(key string, secret string) {
	conf := Config{
		APIKey:    utils.FileContents(key),
		APISecret: utils.FileContents(secret),
	}
	req.Config = conf
	// conf.APIKey := utils.FileContents(key)
	// conf.APISecret := utils.FileContents(secret)
}

// APIKey ...
func (req *Request) APIKey() string {
	return req.Config.APIKey
}

// APISecret ...
func (req *Request) APISecret() string {
	return req.Config.APISecret
}

// NewRequest ...
func NewRequest() *Request {
	r := new(Request)
	r.Parameters = make(map[string]string)
	return r
}

// GenerateSignature ...
func (req *Request) GenerateSignature() {
	var hash []string
	param := make(map[string]string)
	param["api_key"] = req.APIKey()
	param["format"] = Format
	param["expire"] = req.Expire

	// Add the all the endpoint specific parameters
	for key, value := range req.Parameters {
		param[key] = value
	}

	// Sort all the keys alphabetically and then append them to the 'to-be' hash string
	var keys []string
	for k := range param {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		kv := joinKeyValue(k, param[k])
		hash = append(hash, kv)
	}

	// Append api_secret and hash
	hash = append(hash, req.APISecret())
	joinedhash := strings.Join(hash, "")

	req.Signature = GetMD5Hash(joinedhash)
}

// CompileURL ...
func (req *Request) CompileURL(rawflag bool) string {
	var parts, params []string
	if rawflag {
		if len(req.Method) > 0 {
			parts = append(parts, RawEndpoint, Version, req.Endpoint, req.Method)
		} else {
			parts = append(parts, RawEndpoint, Version, req.Endpoint)
		}
	} else {
		if len(req.Method) > 0 {
			parts = append(parts, Endpoint, Version, req.Endpoint, req.Method)
		} else {
			parts = append(parts, Endpoint, Version, req.Endpoint)
		}
	}
	uri := strings.Join(parts, "/")
	uri += "/?"

	for key, value := range req.Parameters {
		kv := joinKeyValue(key, value)
		params = append(params, kv)
	}

	apikey := joinKeyValue("api_key", req.APIKey())
	expire := joinKeyValue("expire", req.Expire)
	format := joinKeyValue("format", Format)
	sig := joinKeyValue("sig", req.Signature)
	params = append(params, apikey, expire, format, sig)

	url := strings.Join(params, "&")

	uri += url
	return uri
}

func joinKeyValue(key string, value string) string {
	var slice []string
	slice = append(slice, key, value)
	kv := strings.Join(slice, "=")
	return kv
}

// CalculateExpiry exp is in seconds
func (req *Request) CalculateExpiry(expire int) string {
	return strconv.FormatInt(time.Now().Add(time.Duration(expire)*time.Second).UTC().Unix(), 10)
}

// GetMD5Hash ...
func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

//////////////
// REQUESTS //
//////////////

// CreateRequest ...
func (req *Request) CreateRequest(raw bool, endpoint string, method string, expire int, params map[string]string) string {
	NewRequest()
	req.Endpoint = endpoint
	req.Method = method
	req.Expire = req.CalculateExpiry(expire)
	for key, value := range params {
		req.Parameters[key] = value
	}
	req.GenerateSignature()
	url := req.CompileURL(raw)
	return url
}

// GetEvents ...
func (req *Request) GetEvents(params map[string]string) string {
	return req.CreateRequest(false, "events", "", 600, params)
}

// GetEventsTop ...
func (req *Request) GetEventsTop(params map[string]string) string {
	return req.CreateRequest(false, "events", "top", 600, params)
}

// GetEventsNames ...
func (req *Request) GetEventsNames(params map[string]string) string {
	return req.CreateRequest(false, "events", "names", 600, params)
}

// GetRawData gets a raw data dump from mixpanel. Required parameters are `from_date`
// and `to_date`, they are both string and in the date format yyyy-mm-dd. Optional
// parameters are `event`, `where`, and `bucket`.
func (req *Request) GetRawData(params map[string]string) string {
	return req.CreateRequest(true, "export", "", 600, params)
}
