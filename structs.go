package lib

import "time"

type Message struct {
	ID          string            `json:"messageId"`
	Data        []byte            `json:"data"`
	Attributes  map[string]string `json:"attributes"`
	PublishTime time.Time         `json:"publishTime"`
}
