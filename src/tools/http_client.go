package tools

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	nurl "net/url"
	"strings"
	"time"
)

var (
	// HTTPNoKeepAliveClient is http client without keep alive
	HTTPNoKeepAliveClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	defaultHTTPClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 2048,
			IdleConnTimeout:     time.Minute * 5,
		},
	}
	defaultTimeout    = 500
	defaultRetryCount = 2
)

// PostRaw PostRaw
func PostRaw(client *http.Client, url string, header http.Header, reqBody interface{}, params ...int) ([]byte, error) {
	var (
		data []byte
		err  error
	)
	timeOut, retryCount := genDefaultParams(params...)
	for i := 0; i < retryCount; i++ {
		data, err = do(client, http.MethodPost, url, header, reqBody, timeOut)
		if err == nil {
			break
		}
	}
	if err != nil {
		fmt.Println(err)
		//log ...
	}
	return data, err
}

// PostWithUnmarshal do http get with unmarshal
func PostWithUnmarshal(client *http.Client, url string, header http.Header, reqBody interface{}, resp interface{}, params ...int) error {
	data, err := PostRaw(client, url, header, reqBody, params...)
	if err != nil {
		return err
	}
	// for no resp needed request.
	if resp == nil {
		return nil
	}
	// for big int
	decoder := JSON.NewDecoder(bytes.NewBuffer(data))
	decoder.UseNumber()
	err = decoder.Decode(resp)
	if err != nil {
		fmt.Println(err)
		//log.Error("PostWithUnmarshal.Decode").Stack().Msgf("err:%s, url:%s, respData:%s", err, url, string(data))
	}
	return err
}

// GetRaw get http raw
func GetRaw(client *http.Client, url string, header http.Header, reqBody interface{}, params ...int) ([]byte, error) {
	var (
		data []byte
		err  error
	)
	timeOut, retryCount := genDefaultParams(params...)
	for i := 0; i < retryCount; i++ {
		data, err = do(client, http.MethodGet, url, header, reqBody, timeOut)
		if err == nil {
			break
		}
	}
	if err != nil {
		fmt.Println(err)
		//log.Error("GetRaw").Stack().Msgf("err:%s", err)
	}
	return data, err
}

// GetWithUnmarshal do http get with unmarshal
func GetWithUnmarshal(client *http.Client, url string, header http.Header, reqBody interface{}, resp interface{}, params ...int) error {
	data, err := GetRaw(client, url, header, reqBody, params...)
	if err != nil {
		return err
	}
	// for no resp needed request.
	if resp == nil {
		return nil
	}
	// for big int
	decoder := JSON.NewDecoder(bytes.NewBuffer(data))
	decoder.UseNumber()
	err = decoder.Decode(resp)
	if err != nil {
		fmt.Println(err)
		//log.Error("GetWithUnmarshal.Decode").Stack().Msgf("err:%s, url:%s, respData:%s", err, url, string(data))
	}
	return err
}

func genDefaultParams(params ...int) (int, int) {
	timeOut, retryCount := defaultTimeout, defaultRetryCount
	switch {
	case len(params) >= 2:
		timeOut, retryCount = params[0], params[1]
	case len(params) >= 1:
		timeOut = params[0]
	}
	return timeOut, retryCount
}

func do(client *http.Client, method string, url string, header http.Header, reqBody interface{}, timeOut int) ([]byte, error) {
	if client == nil {
		client = defaultHTTPClient
	}
	var reader io.Reader
	switch v := reqBody.(type) {
	case nurl.Values:
		reader = strings.NewReader(v.Encode())
	case []byte:
		reader = bytes.NewBuffer(v)
	case string:
		reader = strings.NewReader(v)
	case io.Reader:
		reader = v
	default:
		buff := &bytes.Buffer{}
		err := JSON.NewEncoder(buff).Encode(v)
		if err != nil {
			return nil, err
		}
		reader = buff
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}
	if header != nil {
		req.Header = header
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(timeOut))
	defer cancelFunc()
	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err // TODO maybe should define ctx timeout in package errs
	}
	defer resp.Body.Close()
	// TODO maybe should handle status not equal 200
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}
