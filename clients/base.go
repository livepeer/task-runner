package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"

	"github.com/golang/glog"
)

type baseClient struct {
	httpClient  *http.Client
	baseUrl     string
	baseHeaders http.Header
}

type request struct {
	method, url string
	body        io.Reader
	contentType string
}

type HTTPStatusError struct {
	Status int
	Body   string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("http status %d %s: %q", e.Status, http.StatusText(e.Status), e.Body)
}

func (p *baseClient) doRequest(ctx context.Context, r request, output interface{}) error {
	req, err := p.newRequest(ctx, r)
	if err != nil {
		return err
	}

	client := http.DefaultClient
	if p.httpClient != nil {
		client = p.httpClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if (r.method == "GET" && resp.StatusCode != http.StatusOK) || (resp.StatusCode >= 300) {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			glog.Warningf("Error reading response body: url=%s err=%v", r.url, err)
		}
		return &HTTPStatusError{resp.StatusCode, string(body)}
	}
	if output == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(output)
}

func (p *baseClient) newRequest(ctx context.Context, r request) (*http.Request, error) {
	url := p.baseUrl + r.url
	req, err := http.NewRequestWithContext(ctx, r.method, url, r.body)
	if err != nil {
		return nil, err
	}
	for key, values := range p.baseHeaders {
		req.Header[key] = values
	}
	if r.contentType != "" {
		req.Header.Set("Content-Type", r.contentType)
	}
	return req, nil
}

type part struct {
	name            string
	filename        string
	fileContentType string
	data            io.Reader
}

func multipartBody(parts []part) (body io.ReadCloser, contentType string) {
	body, pipe := io.Pipe()
	mw := multipart.NewWriter(pipe)
	go func() (err error) {
		defer func() { pipe.CloseWithError(err) }()
		for _, p := range parts {
			err = writePart(mw, p.name, p.filename, p.fileContentType, p.data)
			if err != nil {
				return
			}
		}
		err = mw.Close()
		return
	}()
	return body, mw.FormDataContentType()
}

func writePart(mw *multipart.Writer, name, filename, contentType string, data io.Reader) error {
	partw, err := mw.CreatePart(mimeHeader(name, filename, contentType))
	if err != nil {
		return err
	}
	_, err = io.Copy(partw, data)
	return err
}

func mimeHeader(name, filename, contentType string) textproto.MIMEHeader {
	contentDisposition := fmt.Sprintf(`form-data; name=%q`, name)
	if filename != "" {
		contentDisposition += fmt.Sprintf(`; filename=%q`, filename)
	}
	mime := textproto.MIMEHeader{}
	mime.Set("Content-Disposition", contentDisposition)
	if contentType != "" {
		mime.Set("Content-Type", contentType)
	}
	return mime
}
