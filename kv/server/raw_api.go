package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	sr, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	defer sr.Close()
	value, err := sr.GetCF(req.Cf, req.Key)
	return &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: value == nil,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	sr, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer sr.Close()
	value, err := sr.GetCF(req.Cf, req.Key)
	if value == nil {
		return nil, nil
	}
	err = server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	sr, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer sr.Close()
	it := sr.IterCF(req.Cf)
	defer it.Close()
	it.Seek(req.StartKey)
	var kvs []*kvrpcpb.KvPair
	for i := uint32(0); i < req.Limit && it.Valid(); it.Next() {
		value, _ := it.Item().Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   it.Item().Key(),
			Value: value,
		})
		i++
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
