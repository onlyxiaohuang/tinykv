package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	NextKey []byte
	Txn     *MvccTxn
	Iter    engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	scanner := &Scanner{
		NextKey: startKey,
		Txn:     txn,
		Iter:    iter,
	}
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.Iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	iter := scan.Iter
	if !iter.Valid() {
		return nil, nil, nil
	}
	key := scan.NextKey
	iter.Seek(EncodeKey(key, scan.Txn.StartTS))
	if !iter.Valid() {
		return nil, nil, nil
	}
	item := iter.Item()
	gotkey := item.KeyCopy(nil)
	userkey := DecodeUserKey(gotkey)

	if !bytes.Equal(userkey, key) {
		scan.NextKey = userkey
		return scan.Next()
	}

	for {
		iter.Next()
		if !iter.Valid() {
			break
		}
		item2 := iter.Item()
		gotkey2 := item2.KeyCopy(nil)
		userkey2 := DecodeUserKey(gotkey2)
		if !bytes.Equal(userkey2, key) {
			scan.NextKey = userkey2
			break
		}
	}

	val, err := item.ValueCopy(nil)
	if err != nil {
		return key, nil, err
	}
	write, err := ParseWrite(val)
	if err != nil {
		return key, nil, err
	}
	if write == nil {
		return key, nil, nil
	}
	if write.Kind == WriteKindDelete {
		return key, nil, nil
	}
	value, err := scan.Txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	if err != nil {
		return key, nil, err
	}

	return key, value, nil
}
