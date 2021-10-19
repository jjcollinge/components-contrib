package external

import (
	"bytes"
	"context"
	"encoding/gob"
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
func (e *ExternalStore) Delete(deleteReq *state.DeleteRequest) error {
	item := statev1pb.DeleteRequest{
		Key:      deleteReq.Key,
		Metadata: deleteReq.Metadata,
	}

	var opts *common.StateOptions
	if deleteReq.Options.Concurrency != "" {
		if opts == nil {
			opts = &common.StateOptions{}
		}

		opts.Concurrency = ConcurrencyToPb(deleteReq.Options.Concurrency)
	}
	if deleteReq.Options.Consistency != "" {
		if opts == nil {
			opts = &common.StateOptions{}
		}

		opts.Consistency = ConsistencyToPb(deleteReq.Options.Consistency)
	}

	item.Options = opts

	if deleteReq.ETag != nil {
		item.Etag = &common.Etag{
			Value: *deleteReq.ETag,
		}
	}

	_, err := e.client.Delete(context.TODO(), &item)
	return err
}

func (e *ExternalStore) Get(getReq *state.GetRequest) (*state.GetResponse, error) {
	item := statev1pb.GetRequest{
		Key:         getReq.Key,
		Metadata:    getReq.Metadata,
		Consistency: ConsistencyToPb(getReq.Options.Consistency),
	}

	res, err := e.client.Get(context.TODO(), &item)
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

func (e *ExternalStore) Set(setReq *state.SetRequest) error {
	// TODO: Is this a valid decoding?
	val := setReq.Value.([]byte)

	item := statev1pb.SetRequest{
		Key:      setReq.Key,
		Value:    val,
		Metadata: setReq.Metadata,
	}

	var opts *common.StateOptions
	if setReq.Options.Concurrency != "" {
		if opts == nil {
			opts = &common.StateOptions{}
		}

		opts.Concurrency = ConcurrencyToPb(setReq.Options.Concurrency)
	}
	if setReq.Options.Consistency != "" {
		if opts == nil {
			opts = &common.StateOptions{}
		}

		opts.Consistency = ConsistencyToPb(setReq.Options.Consistency)
	}

	item.Options = opts

	if setReq.ETag != nil {
		item.Etag = &common.Etag{
			Value: *setReq.ETag,
		}
	}

	_, err := e.client.Set(context.TODO(), &item)
	return err
}

func (e *ExternalStore) Ping() error {
	_, err := e.client.Ping(context.TODO(), &emptypb.Empty{})
	return err
}

func (e *ExternalStore) BulkDelete(deleteReqs []state.DeleteRequest) error {
	deleteReqsPb := make([]*statev1pb.DeleteRequest, len(deleteReqs))
	for _, delReq := range deleteReqs {
		item := statev1pb.DeleteRequest{
			Key:      delReq.Key,
			Metadata: delReq.Metadata,
		}

		var opts *common.StateOptions
		if delReq.Options.Concurrency != "" {
			if opts == nil {
				opts = &common.StateOptions{}
			}

			opts.Concurrency = ConcurrencyToPb(delReq.Options.Concurrency)
		}
		if delReq.Options.Consistency != "" {
			if opts == nil {
				opts = &common.StateOptions{}
			}

			opts.Consistency = ConsistencyToPb(delReq.Options.Consistency)
		}

		item.Options = opts
		deleteReqsPb = append(deleteReqsPb, &item)
	}
	_, err := e.client.BulkDelete(context.TODO(), &statev1pb.BulkDeleteRequest{
		Items: deleteReqsPb,
	})
	return err
}

func (e *ExternalStore) BulkGet(getReqs []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	getReqsPb := make([]*statev1pb.GetRequest, len(getReqs))
	for _, getReq := range getReqs {
		getReqsPb = append(getReqsPb, &statev1pb.GetRequest{
			Key:         getReq.Key,
			Metadata:    getReq.Metadata,
			Consistency: ConsistencyToPb(getReq.Options.Consistency),
		})
	}
	res, err := e.client.BulkGet(context.TODO(), &statev1pb.BulkGetRequest{
		Items: getReqsPb,
	})
	if err != nil {
		return false, nil, err
	}

	bgr := make([]state.BulkGetResponse, len(res.Items))
	for _, rs := range res.Items {
		bulkGetRes := state.BulkGetResponse{
			Key:      rs.Key,
			Data:     rs.Data,
			Metadata: rs.Metadata,
			Error:    rs.Error,
		}

		if rs.Etag != nil {
			bulkGetRes.ETag = &rs.Etag.Value
		}

		bgr = append(bgr, bulkGetRes)
	}
	return res.Got, bgr, nil
}

func (e *ExternalStore) BulkSet(setReqs []state.SetRequest) error {
	setReqsPb := make([]*statev1pb.SetRequest, len(setReqs))
	for _, setReq := range setReqs {
		// TODO: Fix data encoding/decoding.
		valBytes, err := GetBytes(setReq.Value)
		if err != nil {
			return err
		}

		s := &statev1pb.SetRequest{
			Key:      setReq.Key,
			Metadata: setReq.Metadata,
			Value:    valBytes,
			Options: &common.StateOptions{
				Concurrency: ConcurrencyToPb(setReq.Options.Concurrency),
				Consistency: ConsistencyToPb(setReq.Options.Consistency),
			},
		}

		if setReq.ETag != nil {
			s.Etag = &common.Etag{
				Value: *setReq.ETag,
			}
		}

		setReqsPb = append(setReqsPb, s)
	}

	_, err := e.client.BulkSet(context.TODO(), &statev1pb.BulkSetRequest{
		Items: setReqsPb,
	})

	return err
}

func (e *ExternalStore) Multi(request *state.TransactionalStateRequest) error {
	return errors.New("Not implemented!")
}

func GetBytes(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// TODO: Do this in a better way.
func ConcurrencyToPb(concurrency string) common.StateOptions_StateConcurrency {
	switch strings.ToLower(concurrency) {
	case "unspecified":
		return 0
	case "first_write":
		return 1
	case "last_write":
		return 2
	default:
		return 1 // TODO: What's the right default?
	}
}

// TODO: Do this in a better way.
func ConsistencyToPb(consistency string) common.StateOptions_StateConsistency {
	switch strings.ToLower(consistency) {
	case "unspecified":
		return 0
	case "eventual":
		return 1
	case "strong":
		return 2
	default:
		return 1 // TODO: What's the right default?
	}
}
