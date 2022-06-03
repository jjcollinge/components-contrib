package main

import (
	"fmt"
	"io"
	"os"

	"github.com/dapr/components-contrib/bindings/http"
	"github.com/dapr/components-contrib/internal/component/kafka"
	"github.com/dapr/components-contrib/pubsub/redis"
	pubsub_redis "github.com/dapr/components-contrib/pubsub/redis"
	"github.com/dapr/components-contrib/state/mongodb"
	"github.com/invopop/jsonschema"
)

type componentMetadata struct {
	Type   string
	Schema *jsonschema.Schema
}

func main() {
	var components []componentMetadata

	// bindings
	components = append(components, componentMetadata{
		Type:   "bindings.http",
		Schema: jsonschema.Reflect(http.HttpMetadata{}),
	})

	// state stores
	components = append(components, componentMetadata{
		Type:   "state.mongodb",
		Schema: jsonschema.Reflect(mongodb.MongoDBMetadata{}),
	})
	components = append(components, componentMetadata{
		Type:   "state.mongodb",
		Schema: jsonschema.Reflect(redis.Metadata{}),
	})

	// pubsub
	components = append(components, componentMetadata{
		Type:   "pubsub.redis",
		Schema: jsonschema.Reflect(pubsub_redis.Metadata{}),
	})
	components = append(components, componentMetadata{
		Type:   "pubsub.kafka",
		Schema: jsonschema.Reflect(kafka.KafkaMetadata{}),
	})

	for _, component := range components {
		f, err := os.Create(fmt.Sprintf("%s.json", component.Type))
		if err != nil {
			panic(err)
		}
		err = writeSchema(component.Schema, f)
		if err != nil {
			panic(err)
		}
	}
}

func writeSchema(schema *jsonschema.Schema, w io.Writer) error {
	b, err := schema.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %v", err)
	}
	_, err = w.Write(b)
	if err != nil {
		return fmt.Errorf("failed to write schema: %v", err)
	}
	return nil
}
