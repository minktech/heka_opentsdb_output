package heka_opentsdb_output

import (
	"bosun.org/opentsdb"
	"encoding/json"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"time"

	. "github.com/mozilla-services/heka/pipeline"
)

type OpenTsdbEncoder struct {
	*OpenTsdbEncoderConfig
}

type OpenTsdbEncoderConfig struct {
	//the decode method, there are three methods:"json","bson","raw",
	//json decode message use Tags config and Value config
	//bson decode as json but treat payload as bson object
	//raw decode value from payload and tags from message
	Decode string
	Tags   []string
	Values []string
	//the metric root
	Metric     string
	TagIfEmpty map[string]string `toml:"tag_for_empty"`
}

func (e *OpenTsdbEncoder) ConfigStruct() interface{} {
	return &OpenTsdbEncoderConfig{
		Decode: "raw",
		Values: []string{"payload"},
	}
}

func (e *OpenTsdbEncoder) Init(config interface{}) (err error) {
	e.OpenTsdbEncoderConfig = config.(*OpenTsdbEncoderConfig)
	return
}

func (e *OpenTsdbEncoder) Encode(pack *PipelinePack) (output []byte, err error) {

	payload := pack.Message.GetPayload()
	tagMsg := make(map[string]interface{})

	dp := make([]*opentsdb.DataPoint, len(e.Values))

	switch e.OpenTsdbEncoderConfig.Decode {
	case "json":
		if err = json.Unmarshal([]byte(payload), &tagMsg); err != nil {
			return nil, err
		}
		goto j_b_son_get_value
	case "bson":
		if err = bson.Unmarshal([]byte(payload), &tagMsg); err != nil {
			return nil, err
		}
		goto j_b_son_get_value
	default:
		for i := 0; i < len(e.Values); i++ {
			dp[i] = new(opentsdb.DataPoint)
			if e.Values[i] == "payload" {
				dp[i].Value = payload
				dp[i].Metric = e.Metric
			} else {
				var ok bool
				if dp[i].Value, ok = pack.Message.GetFieldValue(e.Values[i]); ok {
					dp[i].Metric = e.Metric + "." + e.Values[i]
				} else {
					dp[i] = nil
				}
			}
		}
		for _, v := range e.Tags {
			if tag, ok := tagMsg[v]; ok {
				dp[0].Tags[v] = fmt.Sprintf("%v", tag)
			}
		}
		goto copy_0_tags_to_n
	}

j_b_son_get_value:
	for i := 0; i < len(e.Values); i++ {
		dp[i] = new(opentsdb.DataPoint)
		dp[i].Value = tagMsg[e.Values[i]]
		dp[i].Metric = e.Metric + "." + e.Values[i]
	}
	dp[0].Tags = make(opentsdb.TagSet)
	for _, v := range e.Tags {
		if tag, ok := tagMsg[v]; ok {
			dp[0].Tags[v] = fmt.Sprintf("%v", tag)
		}
	}
	// clean_empty_tag:
	for k, v := range dp[0].Tags {
		if v == "" {
			if tagForEmpty, ok := e.TagIfEmpty[k]; ok {
				dp[0].Tags[k] = tagForEmpty
			}
		}
	}
copy_0_tags_to_n:
	for i := 0; i < len(dp); i++ {
		if dp[i] != nil {
			dp[i].Tags = dp[0].Tags
			dp[i].Timestamp = time.Now().Unix()
		}
	}

	dp_non_empty := make([]*opentsdb.DataPoint, 0)
	for _, v := range dp {
		if v.Value != nil && v.Tags != nil {
			dp_non_empty = append(dp_non_empty, v)
		}
	}

	if bts, err := json.Marshal(dp_non_empty); err == nil {
		bts = bts[1:]
		bts = bts[:len(bts)-1]
		return bts, err
	} else {
		return bts, err
	}
}

func init() {
	RegisterPlugin("OpenTsdbEncoder", func() interface{} {
		return new(OpenTsdbEncoder)
	})
}
