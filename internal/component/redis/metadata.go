/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package redis

import (
	"fmt"
	"strconv"
	"time"
)

const (
	maxRetries             = "maxRetries"
	maxRetryBackoff        = "maxRetryBackoff"
	ttlInSeconds           = "ttlInSeconds"
	queryIndexes           = "queryIndexes"
	defaultBase            = 10
	defaultBitSize         = 0
	defaultMaxRetries      = 3
	defaultMaxRetryBackoff = time.Second * 2
)

type Metadata struct {
	MaxRetries      int           `jsonschema:"title=max retries,description=The max retries, example=3"`
	MaxRetryBackoff time.Duration `jsonschema:"title=max retry backoff,description=The max retry backoff, example=2s"`
	TTLInSeconds    *int          `jsonschema:"title=ttl in seconds,description=The ttl in seconds, example=100"`
	QueryIndexes    string        `jsonschema:"title=query indexes,description=The query indexes, example=my-index"`
}

func ParseRedisMetadata(properties map[string]string) (Metadata, error) {
	m := Metadata{}

	m.MaxRetries = defaultMaxRetries
	if val, ok := properties[maxRetries]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse maxRetries field: %s", err)
		}
		m.MaxRetries = int(parsedVal)
	}

	m.MaxRetryBackoff = defaultMaxRetryBackoff
	if val, ok := properties[maxRetryBackoff]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse maxRetryBackoff field: %s", err)
		}
		m.MaxRetryBackoff = time.Duration(parsedVal)
	}

	if val, ok := properties[ttlInSeconds]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse ttlInSeconds field: %s", err)
		}
		intVal := int(parsedVal)
		m.TTLInSeconds = &intVal
	} else {
		m.TTLInSeconds = nil
	}

	if val, ok := properties[queryIndexes]; ok && val != "" {
		m.QueryIndexes = val
	}
	return m, nil
}
