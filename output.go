package heka_opentsdb_output

import (
	"bytes"
	"compress/gzip"
	"errors"
	"github.com/mozilla-services/heka/pipeline"
	"gopkg.in/mgo.v2"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

type timeoutTransport struct {
	*http.Transport
	Timeout time.Time
}

func (t *timeoutTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if time.Now().After(t.Timeout) {
		t.Transport.CloseIdleConnections()
		t.Timeout = time.Now().Add(time.Minute * 5)
	}
	return t.Transport.RoundTrip(r)
}

type OpenTsdbOutput struct {
	*OpenTsdbOutputConfig
	mgoSession *mgo.Session
	logMsgChan chan []byte
	client     *http.Client
}

type OpenTsdbOutputConfig struct {
	LogMsgChSize    int
	TsdbWritingSize int
	//the writer count
	TsdbWriterCount int
	//the time period of writing data to opentsdb
	TsdbWriteTimeout int
	//the basename for all metrics
	Root string
	//the url of opentsdb server
	Url string
}

func (o *OpenTsdbOutput) ConfigStruct() interface{} {
	return &OpenTsdbOutputConfig{
		Url:             "127.0.0.1:4242",
		LogMsgChSize:    10000,
		TsdbWritingSize: 20,
		TsdbWriterCount: runtime.NumCPU(),
	}
}

func (o *OpenTsdbOutput) Init(config interface{}) (err error) {
	o.OpenTsdbOutputConfig = config.(*OpenTsdbOutputConfig)
	//todo: check address validity
	// if o.url, err = url.Parse(o.Address); err != nil {
	//     return fmt.Errorf("Can't parse URL '%s': %s", o.Address, err.Error())
	// }
	//
	o.client = &http.Client{
		Transport: &timeoutTransport{Transport: new(http.Transport)},
		Timeout:   time.Minute,
	}

	var u *url.URL
	if u, err = url.Parse(o.Url); err == nil {
	}

	o.logMsgChan = make(chan []byte, o.LogMsgChSize)

	u, err = u.Parse("/api/put")
	if err != nil {
		return err
	}
	if strings.HasPrefix(u.Host, ":") {
		u.Host = "localhost" + u.Host
	}
	o.Url = u.String()

	if err != nil {
		log.Printf("initialize OpenTsdbOutput failed, %s", err.Error())
		return err
	}
	return
}

func (o *OpenTsdbOutput) SendDataPoints(dps [][]byte, tsdb string) (*http.Response, error) {
	var buf bytes.Buffer
	g := gzip.NewWriter(&buf)

	//handle bytes
	g.Write([]byte("["))
	g.Write(bytes.Join(dps, []byte(",")))
	g.Write([]byte("]"))

	if err := g.Close(); err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", tsdb, &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := o.client.Do(req)
	return resp, err
}

func WriteDataToOpenTSDB(mo *OpenTsdbOutput) {
	defer func() {
		if err, ok := recover().(error); ok {
			log.Println("WARN: panic in %v", err)
			log.Println(string(debug.Stack()))
		}
	}()

	//srvlog.Printf("WriteDataToOpenTSDB key:%s", key)
	cache := make([][]byte, mo.TsdbWritingSize)
	lastWrite := time.Now()
	i := 0

	for logMsg := range mo.logMsgChan {
		if i >= mo.TsdbWritingSize || time.Now().Sub(lastWrite) > time.Second*time.Duration(mo.TsdbWriteTimeout) {
			resp, err := mo.SendDataPoints(cache, mo.Url)
			if err != nil || resp.StatusCode != http.StatusNoContent {
				//restore
				for _, v := range cache {
					mo.logMsgChan <- v
					time.Sleep(time.Second * 5)
				}
			}
			i = 0
		}
		cache[i] = logMsg
		i++
	}
}

func (o *OpenTsdbOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) (err error) {
	if or.Encoder() == nil {
		return errors.New("Encoder must be specified.")
	}

	var (
		e        error
		outBytes []byte
	)
	inChan := or.InChan()

	for i := 0; i < o.TsdbWriterCount; i++ {
		go WriteDataToOpenTSDB(o)
	}

	for pack := range inChan {
		outBytes, e = or.Encode(pack)
		pack.Recycle(e)
		if e != nil {
			or.LogError(e)
			continue
		}
		if outBytes == nil {
			continue
		}

		if e != nil {
			log.Printf("OpenTsdbOutput-%s", e.Error())
			continue
		}

		//fmt.Printf("OpenTsdbOutput-165-%v", logMsg)
		o.logMsgChan <- outBytes
		//fmt.Println("OpenTsdbOutput:", string(outBytes))
	}

	return
}

func init() {
	pipeline.RegisterPlugin("OpenTsdbOutput", func() interface{} {
		return new(OpenTsdbOutput)
	})
}
