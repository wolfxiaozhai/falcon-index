package index

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"falcon-index/doc"
	"falcon-index/g"
	cmodel "github.com/open-falcon/common/model"
	cutils "github.com/open-falcon/common/utils"
	"github.com/toolkits/file"
	"log"
	"strings"
	"sort"
)


func SortedTags(tag_string string) string {
	if tag_string == "" {
		return ""
	}
	tag_list := strings.Split(tag_string, ",")
	size := len(tag_list)
	tags := make(map[string]string, size)
	for _, v := range(tag_list){
		k_v := strings.Split(v, "=")
		if len(k_v) == 2{
			tags[k_v[0]] = k_v[1]
		}
	}
	if size == 0 {
		return ""
	}

	if size == 1 {
		for k, v := range tags {
			return fmt.Sprintf("%s=%s", k, v)
		}
	}

	keys := make([]string, size)
	i := 0
	for k := range tags {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	ret := make([]string, size)
	for j, key := range keys {
		ret[j] = fmt.Sprintf("%s=%s", key, tags[key])
	}

	return strings.Join(ret, ",")
}


// each term as a bucket, for seek speedup, and save doc together
func HttpBuildIndex(content string) error {
	reason := fmt.Errorf("no error")
	endpoint_bname := []byte(g.ENDPOINT_NAME_BUCKET)
	metric_bname := []byte(g.METRIC_NAME_BUCKET)
	g.KVDB.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(endpoint_bname)
		tx.CreateBucketIfNotExists(metric_bname)
		return nil
	})

	testContent := content
	var data []*cmodel.JsonMetaData
	err := json.Unmarshal([]byte(testContent), &data)
	if err != nil {
		reason = fmt.Errorf("parse test file error:%s", err.Error())
		return reason
	}

	for _, jmd := range data {
		d := &doc.MetaDoc{
			Endpoint:    proto.String(jmd.Endpoint),
			Metric:      proto.String(jmd.Metric),
			CounterType: proto.String(jmd.CounterType),
			Step:        proto.Int64(jmd.Step),
			Tags:        []*doc.Pair{},
		}
		tags := cutils.DictedTagstring(jmd.Tags)
		sortedTags := cutils.SortedTags(tags)
		for tagk, tagv := range tags {
			p := &doc.Pair{
				Key:   proto.String(tagk),
				Value: proto.String(tagv),
			}
			d.Tags = append(d.Tags, p)
		}
		log.Printf("doc:%v\n", d)
		doc_bytes, err := d.Marshal()
        if err != nil {
            return fmt.Errorf("marshal doc:%s", err)
        }

		g.KVDB.Update(func(tx *bolt.Tx) error {

			tmp_metric := d.GetMetric()
			mb := tx.Bucket([]byte(metric_bname))
			if mb == nil {
				return fmt.Errorf("no such bucket:%s", metric_bname)
			}
			mb.Put([]byte(tmp_metric), []byte(""))

			tmp_endpoint_name := d.GetEndpoint()
			eb := tx.Bucket([]byte(endpoint_bname))
			if eb == nil {
				return fmt.Errorf("no such bucket:%s", endpoint_bname)
			}
			eb.Put([]byte(tmp_endpoint_name), []byte(""))

			//bucket name is endpoint_name, key为metric/sortedTags, value为[]byte""
			endpoint_bucket, err:= tx.CreateBucketIfNotExists([]byte(tmp_endpoint_name))
			if err != nil {
				return fmt.Errorf("create endpint_bucket: %s", err)
			}
			counter := tmp_metric + "/" + sortedTags
			endpoint_bucket.Put([]byte(counter), doc_bytes)

			var buf bytes.Buffer
			for k, v := range tags {
				buf.Reset()
				buf.WriteString(k)
				buf.WriteString("=")
				buf.WriteString(v)
				term := buf.Bytes()

				tag_bucket, err := tx.CreateBucketIfNotExists(term)
				if err != nil {
					return fmt.Errorf("create tag bucket:%s", err)
				}
				tag_bucket.Put([]byte(tmp_endpoint_name), []byte(""))
			}
			return nil
		})
	}
	return reason
}

func BuildIndex() {
	endpoint_bname := []byte(g.ENDPOINT_NAME_BUCKET)
	metric_bname := []byte(g.METRIC_NAME_BUCKET)
	g.KVDB.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(endpoint_bname)
		tx.CreateBucketIfNotExists(metric_bname)
		return nil
	})

	testContent, err := file.ToTrimString("./test-metadata.json")
	if err != nil {
		log.Fatalln("read test data file error:", err.Error())
	}
	var data []*cmodel.JsonMetaData
	err = json.Unmarshal([]byte(testContent), &data)
	if err != nil {
		log.Fatalln("parse test file error:", err.Error())
	}
	for _, jmd := range data {
		d := &doc.MetaDoc{
			Endpoint:    proto.String(jmd.Endpoint),
			Metric:      proto.String(jmd.Metric),
			CounterType: proto.String(jmd.CounterType),
			Step:        proto.Int64(jmd.Step),
			Tags:        []*doc.Pair{},
		}
		tags := cutils.DictedTagstring(jmd.Tags)
		sortedTags := cutils.SortedTags(tags)
		for tagk, tagv := range tags {
			p := &doc.Pair{
				Key:   proto.String(tagk),
				Value: proto.String(tagv),
			}
			d.Tags = append(d.Tags, p)
		}
		log.Printf("doc:%v\n", d)
		doc_bytes, err := d.Marshal()
        if err != nil {
            log.Fatalln("marshal doc:%s", err.Error())
        }

		g.KVDB.Update(func(tx *bolt.Tx) error {

			tmp_metric := d.GetMetric()
			mb := tx.Bucket([]byte(metric_bname))
			if mb == nil {
				return fmt.Errorf("no such bucket:%s", metric_bname)
			}
			mb.Put([]byte(tmp_metric), []byte(""))

			tmp_endpoint_name := d.GetEndpoint()
			eb := tx.Bucket([]byte(endpoint_bname))
			if eb == nil {
				return fmt.Errorf("no such bucket:%s", endpoint_bname)
			}
			eb.Put([]byte(tmp_endpoint_name), []byte(""))

			//bucket name is endpoint_name, key为metric/sortedTags, value为[]byte""
			endpoint_bucket, err:= tx.CreateBucketIfNotExists([]byte(tmp_endpoint_name))
			if err != nil {
				return fmt.Errorf("create endpint_bucket: %s", err)
			}
			counter := tmp_metric + "/" + sortedTags
			endpoint_bucket.Put([]byte(counter), doc_bytes)

			var buf bytes.Buffer
			for k, v := range tags {
				buf.Reset()
				buf.WriteString(k)
				buf.WriteString("=")
				buf.WriteString(v)
				term := buf.Bytes()

				tag_bucket, err := tx.CreateBucketIfNotExists(term)
				if err != nil {
					return fmt.Errorf("create tag bucket:%s", err)
				}
				tag_bucket.Put([]byte(tmp_endpoint_name), []byte(""))
			}
			return nil
		})
	
	}
}

