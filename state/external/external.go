package external

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/dapr/components-contrib/state"
	statev1pb "github.com/dapr/components-contrib/state/proto/v1"
	"github.com/dapr/dapr/pkg/proto/common/v1"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	externalAddressMetadataKey = "externalAddress"
)

type ExternalStore struct {
	client statev1pb.StoreClient
}

func New() *ExternalStore {
	// Cannot initialize gRPC client here as don't have metadata.
	return &ExternalStore{}
}

func (e *ExternalStore) Init(metadata state.Metadata) error {
	// TODO: Define a better convention for loaded this config.
	if metadata.Properties[externalAddressMetadataKey] == "" {
		return errors.New("external state store: service address missing.")
	}
	address := metadata.Properties[externalAddressMetadataKey]

	// Remove external address key from metadata
	delete(metadata.Properties, externalAddressMetadataKey)

	// TODO:
	// * Need a Close method to close the gRPC connection.
	// * Security
	// * Tracing
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	e.client = statev1pb.NewStoreClient(conn)

	req := statev1pb.MetadataRequest{
		Properties: metadata.Properties,
	}
	_, err = e.client.Init(context.TODO(), &req)
	return errors.Wrap(err, "error calling remote init")
}

func (e *ExternalStore) Features() []state.Feature {
	fs, err := e.client.Features(context.TODO(), &emptypb.Empty{})
	if err != nil {
		return nil
	}

	// Map Feautre back to string - TODO: can we do this better.
	var features []state.Feature
	for _, f := range fs.Feature {
		if f == string(state.FeatureETag) {
			features = append(features, state.FeatureETag)
		}
		if f == string(state.FeatureTransactional) {
			features = append(features, state.FeatureTransactional)
		}
	}

	return features
}

func (e *ExternalStore) Delete(req *state.DeleteRequest) error {
	delReq := statev1pb.DeleteRequest{
		Key:      req.Key,
		Metadata: req.Metadata,
		Options:  stateOptionsPbFromOptions(req.Options.Concurrency, req.Options.Consistency),
	}
	if req.ETag != nil {
		delReq.Etag = &common.Etag{
			Value: *req.ETag,
		}
	}

	_, err := e.client.Delete(context.TODO(), &delReq)
	return err
}

func (e *ExternalStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	getReq := statev1pb.GetRequest{
		Key:         req.Key,
		Metadata:    req.Metadata,
		Consistency: stateConsistencyFromString(req.Options.Consistency),
	}

	res, err := e.client.Get(context.TODO(), &getReq)
	if err != nil {
		return nil, err
	}
	getRes := state.GetResponse{
		Data:     res.Data,
		Metadata: res.Metadata,
	}
	if res.Etag != nil {
		getRes.ETag = &res.Etag.Value
	}

	return &getRes, nil
}

func (e *ExternalStore) Set(req *state.SetRequest) error {
	// TODO: Is this a valid decoding?
	val := req.Value.([]byte)

	setReq := statev1pb.SetRequest{
		Key:      req.Key,
		Value:    val,
		Metadata: req.Metadata,
		Options:  stateOptionsPbFromOptions(req.Options.Concurrency, req.Options.Consistency),
	}
	if req.ETag != nil {
		setReq.Etag = &common.Etag{
			Value: *req.ETag,
		}
	}

	_, err := e.client.Set(context.TODO(), &setReq)
	return err
}

func (e *ExternalStore) Ping() error {
	_, err := e.client.Ping(context.TODO(), &emptypb.Empty{})
	return err
}

func (e *ExternalStore) BulkDelete(reqs []state.DeleteRequest) error {
	deleteReqs := make([]*statev1pb.DeleteRequest, len(reqs))
	for _, req := range reqs {
		deleteReq := statev1pb.DeleteRequest{
			Key:      req.Key,
			Metadata: req.Metadata,
			Options:  stateOptionsPbFromOptions(req.Options.Concurrency, req.Options.Consistency),
		}
		deleteReqs = append(deleteReqs, &deleteReq)
	}
	_, err := e.client.BulkDelete(context.TODO(), &statev1pb.BulkDeleteRequest{
		Items: deleteReqs,
	})
	return err
}

func (e *ExternalStore) BulkGet(reqs []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	getReqs := make([]*statev1pb.GetRequest, len(reqs))
	for _, req := range reqs {
		getReqs = append(getReqs, &statev1pb.GetRequest{
			Key:         req.Key,
			Metadata:    req.Metadata,
			Consistency: stateConsistencyFromString(req.Options.Consistency),
		})
	}
	res, err := e.client.BulkGet(context.TODO(), &statev1pb.BulkGetRequest{
		Items: getReqs,
	})
	if err != nil {
		return false, nil, err
	}

	bulkGetResponses := make([]state.BulkGetResponse, len(res.Items))
	for _, item := range res.Items {
		bulkGetRes := state.BulkGetResponse{
			Key:      item.Key,
			Data:     item.Data,
			Metadata: item.Metadata,
			Error:    item.Error,
		}
		if item.Etag != nil {
			bulkGetRes.ETag = &item.Etag.Value
		}

		bulkGetResponses = append(bulkGetResponses, bulkGetRes)
	}
	return res.Got, bulkGetResponses, nil
}

func (e *ExternalStore) BulkSet(reqs []state.SetRequest) error {
	setReqs := make([]*statev1pb.SetRequest, len(reqs))
	for _, req := range reqs {
		var bytes []byte
		var err error
		if req.Value != nil {
			// TODO:
			// How should we encode the data to send to the
			// external state store? It's already been unmarshalled
			// in a Go struct here so we just marshal it back to a
			// byte array for now and expect the remote state store
			// to handle it.
			bytes, err = json.Marshal(req.Value)
			if err != nil {
				return err
			}
		}

		setReq := &statev1pb.SetRequest{
			Key:      req.Key,
			Metadata: req.Metadata,
			Value:    bytes,
			Options: &common.StateOptions{
				Concurrency: stateConcurrencyFromString(req.Options.Concurrency),
				Consistency: stateConsistencyFromString(req.Options.Consistency),
			},
		}
		if req.ETag != nil {
			setReq.Etag = &common.Etag{
				Value: *req.ETag,
			}
		}
		setReqs = append(setReqs, setReq)
	}

	_, err := e.client.BulkSet(context.TODO(), &statev1pb.BulkSetRequest{
		Items: setReqs,
	})

	return err
}

func (e *ExternalStore) Multi(request *state.TransactionalStateRequest) error {
	return errors.New("Not implemented!")
}

// TODO: Is there a better or existing way to map this?
func stateConcurrencyFromString(concurrency string) common.StateOptions_StateConcurrency {
	switch strings.ToLower(concurrency) {
	case "unspecified":
		return 0
	case "first_write":
		return 1
	case "last_write":
		return 2
	default:
		return 0
	}
}

// TODO: Is there a better or existing way to map this?
func stateConsistencyFromString(consistency string) common.StateOptions_StateConsistency {
	switch strings.ToLower(consistency) {
	case "unspecified":
		return 0
	case "eventual":
		return 1
	case "strong":
		return 2
	default:
		return 0
	}
}

// TODO: Is there a better or existing way to map this?
func stateOptionsPbFromOptions(concurrency, consistency string) *common.StateOptions {
	var opts common.StateOptions
	if concurrency != "" {
		opts.Concurrency = stateConcurrencyFromString(concurrency)
	}
	if consistency != "" {
		opts.Consistency = stateConsistencyFromString(consistency)
	}

	return &opts
}
