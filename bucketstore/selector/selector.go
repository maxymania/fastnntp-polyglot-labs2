/*
Copyright (c) 2018 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package selector

//import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/bucketmap"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/netkv"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/selerr"

var notImpl = selerr.ENotImplemented
var notBucket = selerr.ENoSuchBucket

type Selector struct {
	BM *bucketmap.BucketMap
	NKV *netkv.NetKVMap
}
func (s *Selector) BucketGet(bucket, key []byte) (bufferex.Binary, error) {
	if b := s.NKV.Get(bucket); b!=nil {
		if b.Reader==nil { return bufferex.Binary{},notImpl }
		return b.Reader.BucketGet(bucket,key)
	}
	b,ok := s.BM.Obtain(bucket)
	if !ok { return bufferex.Binary{},notBucket }
	if b.Reader==nil { return bufferex.Binary{},notImpl }
	return b.Reader.BucketGet(bucket,key)
}
func (s *Selector) BucketPut(bucket, key, value []byte) error {
	if b := s.NKV.Get(bucket); b!=nil {
		if b.Writer==nil { return notImpl }
		return b.Writer.BucketPut(bucket,key,value)
	}
	b,ok := s.BM.Obtain(bucket)
	if !ok { return notBucket }
	if b.Writer==nil { return notImpl }
	return b.Writer.BucketPut(bucket,key,value)
}
func (s *Selector) BucketDelete(bucket, key []byte) error {
	if b := s.NKV.Get(bucket); b!=nil {
		if b.Writer==nil { return notImpl }
		return b.Writer.BucketDelete(bucket,key)
	}
	b,ok := s.BM.Obtain(bucket)
	if !ok { return notBucket }
	if b.Writer==nil { return notImpl }
	return b.Writer.BucketDelete(bucket,key)
}
func (s *Selector) BucketPutExpire(bucket, key, value []byte, expiresAt uint64) error {
	if b := s.NKV.Get(bucket); b!=nil {
		if b.WriterEx==nil { return notImpl }
		return b.WriterEx.BucketPutExpire(bucket,key,value,expiresAt)
	}
	b,ok := s.BM.Obtain(bucket)
	if !ok { return notBucket }
	if b.WriterEx==nil { return notImpl }
	return b.WriterEx.BucketPutExpire(bucket,key,value,expiresAt)
}

//
