package external

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/dapr/components-contrib/state"
	statev1pb "github.com/dapr/components-contrib/state/proto/v1"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
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
	if metadata.Properties["external:address"] == "" {
		return errors.New("external state store: service address missing.")
	}
	address := metadata.Properties["external:address"]

	// TODO: Need a Close method to close the gRPC connection.
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
	deleteReqPb := statev1pb.DeleteRequest{
		Key:         deleteReq.Key,
		Etag:        GetEtag(deleteReq.ETag),
		Metadata:    deleteReq.Metadata,
		Concurrency: deleteReq.Options.Concurrency,
		Consistency: deleteReq.Options.Consistency,
	}

	_, err := e.client.Delete(context.TODO(), &deleteReqPb)
	return err
}

func (e *ExternalStore) Get(getReq *state.GetRequest) (*state.GetResponse, error) {
	getReqPb := statev1pb.GetRequest{
		Key:         getReq.Key,
		Metadata:    getReq.Metadata,
		Consistency: getReq.Options.Consistency,
	}

	res, err := e.client.Get(context.TODO(), &getReqPb)
	if err != nil {
		return nil, err
	}
	return &state.GetResponse{
		Data:     res.Data,
		ETag:     &res.Etag,
		Metadata: res.Metadata,
	}, nil
}

func (e *ExternalStore) Set(setReq *state.SetRequest) error {
	// TODO: Is this a valid decoding?
	val := setReq.Value.([]byte)

	setReqPb := statev1pb.SetRequest{
		Key:         setReq.Key,
		Value:       val,
		Etag:        GetEtag(setReq.ETag),
		Metadata:    setReq.Metadata,
		Concurrency: setReq.Options.Concurrency,
		Consistency: setReq.Options.Consistency,
	}

	_, err := e.client.Set(context.TODO(), &setReqPb)
	return err
}

func (e *ExternalStore) Ping() error {
	_, err := e.client.Ping(context.TODO(), &emptypb.Empty{})
	return err
}

func (e *ExternalStore) BulkDelete(deleteReq []state.DeleteRequest) error {
	deleteReqsPb := make([]*statev1pb.DeleteRequest, len(deleteReq))
	for _, dr := range deleteReq {
		deleteReqsPb = append(deleteReqsPb, &statev1pb.DeleteRequest{
			Key:         dr.Key,
			Metadata:    dr.Metadata,
			Consistency: dr.Options.Consistency,
			Concurrency: dr.Options.Concurrency,
		})
	}
	_, err := e.client.BulkDelete(context.TODO(), &statev1pb.BulkDeleteRequest{
		Requests: deleteReqsPb,
	})
	return err
}

func (e *ExternalStore) BulkGet(getReq []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	getReqsPb := make([]*statev1pb.GetRequest, len(getReq))
	for _, gr := range getReq {
		getReqsPb = append(getReqsPb, &statev1pb.GetRequest{
			Key:         gr.Key,
			Metadata:    gr.Metadata,
			Consistency: gr.Options.Consistency,
		})
	}
	res, err := e.client.BulkGet(context.TODO(), &statev1pb.BulkGetRequest{
		Requests: getReqsPb,
	})
	if err != nil {
		return false, nil, err
	}

	bgr := make([]state.BulkGetResponse, len(res.Responses))
	for _, rs := range res.Responses {
		bgr = append(bgr, state.BulkGetResponse{
			Key:      rs.Key,
			Data:     rs.Data,
			Metadata: rs.Metadata,
			ETag:     &rs.Etag,
			Error:    rs.Error,
		})
	}
	return res.Got, bgr, nil
}

func (e *ExternalStore) BulkSet(setReq []state.SetRequest) error {
	setReqsPb := make([]*statev1pb.SetRequest, len(setReq))
	for _, sr := range setReq {
		// TODO: Is this a valid decoding?
		valBytes, err := GetBytes(sr.Value)
		if err != nil {
			return err
		}

		setReqsPb = append(setReqsPb, &statev1pb.SetRequest{
			Key:         sr.Key,
			Etag:        GetEtag(sr.ETag),
			Metadata:    sr.Metadata,
			Value:       valBytes,
			Concurrency: sr.Options.Concurrency,
			Consistency: sr.Options.Consistency,
		})
	}

	_, err := e.client.BulkSet(context.TODO(), &statev1pb.BulkSetRequest{
		Requests: setReqsPb,
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

func GetEtag(etagPtr *string) string {
	var etag string
	if etagPtr != nil {
		etag = *etagPtr
	}
	return etag
}
