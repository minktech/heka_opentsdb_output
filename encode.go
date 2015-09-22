package heka_opentsdb_output

import (
	"bosun.org/opentsdb"
	"encoding/json"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"strings"
	"time"

	. "github.com/mozilla-services/heka/pipeline"
)

type OpenTsdbEncoder struct {
	*OpenTsdbEncoderConfig
	Tags []string
}

type OpenTsdbEncoderConfig struct {
	//the decode method, there are three methods:"json","bson","raw",
	//json decode message use Tags config and Value config
	//bson decode as json but treat payload as bson object
	//raw decode value from payload and tags from message
	Decode   string
	Tags_str string `toml:"tags"`
	Value    string
	//the metric root
	Metric string
}

func (e *OpenTsdbEncoder) ConfigStruct() interface{} {
	return &OpenTsdbEncoderConfig{
		Decode: "raw",
		Value:  "payload",
	}
}

func (e *OpenTsdbEncoder) Init(config interface{}) (err error) {
	e.OpenTsdbEncoderConfig = config.(*OpenTsdbEncoderConfig)
	e.Tags = strings.Split(e.Tags_str, ",")
	return
}

func (e *OpenTsdbEncoder) Encode(pack *PipelinePack) (output []byte, err error) {

	payload := pack.Message.GetPayload()
	tagMsg := make(map[string]interface{})

	var dp opentsdb.DataPoint

	switch e.OpenTsdbEncoderConfig.Decode {
	case "json":
		if err = json.Unmarshal([]byte(payload), &tagMsg); err != nil {
			return nil, err
		} else {
			dp.Value = tagMsg[e.Value]
			for _, v := range e.Tags {
				if tag, ok := tagMsg[v]; ok {
					dp.Tags[v] = fmt.Sprintf("%v", tag)
				}
			}
		}
	case "bson":
		if err = bson.Unmarshal([]byte(payload), &tagMsg); err != nil {
			return nil, err
		} else {
			dp.Value = tagMsg[e.Value]
			for _, v := range e.Tags {
				if tag, ok := tagMsg[v]; ok {
					dp.Tags[v] = fmt.Sprintf("%v", tag)
				}
			}
		}
	default:
		dp.Value = payload
		for _, v := range e.Tags {
			if tag, ok := pack.Message.GetFieldValue(v); ok {
				dp.Tags[v] = fmt.Sprintf("%v", tag)
			}
		}
	}

	dp.Metric = e.Metric
	dp.Timestamp = time.Now().Unix()

	return json.Marshal(dp)
}

func init() {
	RegisterPlugin("OpenTsdbEncoder", func() interface{} {
		return new(OpenTsdbEncoder)
	})
}
