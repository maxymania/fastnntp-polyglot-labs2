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


package netkinetic

import "bytes"
import kinetic "github.com/Kinetic/kinetic-go"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/netkv"
import "github.com/vmihailenco/msgpack"


type Bucket struct {
	Cli *kinetic.BlockConnection
}
func (b *Bucket) Close() error {
	b.Cli.Close()
	return nil
}
func (b *Bucket) BucketGet(bucket, key []byte) (buf bufferex.Binary, err error) {
	r,s,e := b.Cli.Get(key)
	if e==nil { err = e; return }
	if s.Code!=kinetic.OK { err = s; return }
	buf = bufferex.NewBinaryInplace(r.Value)
	return
}
var _ bucketstore.BucketR = (*Bucket)(nil)

func (b *Bucket) BucketPut(bucket, key, value []byte) error {
	s,e := b.Cli.Put(&kinetic.Record{
		Key:key,
		Value:value,
		Sync:kinetic.SyncWriteBack,
		Algo:kinetic.AlgorithmSHA3,
		Force:true,
	})
	if e!=nil { return e }
	if s.Code!=kinetic.OK { return s }
	return nil
}
func (b *Bucket) BucketDelete(bucket, key []byte) error {
	s,e := b.Cli.Delete(&kinetic.Record{
		Key:key,
		Sync:kinetic.SyncWriteBack,
		Force:true,
	})
	if e!=nil { return e }
	if s.Code!=kinetic.OK { return s }
	return nil
}
var _ bucketstore.BucketW = (*Bucket)(nil)

const PROVIDER = "kinetic"

func Generate(co kinetic.ClientOptions) []byte {
	var buf bytes.Buffer
	err := msgpack.NewEncoder(&buf).EncodeMulti(
		co.Host,
		co.Port,
		co.User,
		co.Hmac,
		co.UseSSL,
		co.Timeout,
		co.RequestTimeout,
	)
	if err!=nil { panic(err) }
	return buf.Bytes()
}
func loader(bucket, meta []byte) (*netkv.Session, error) {
	var co kinetic.ClientOptions
	err := msgpack.NewDecoder(bytes.NewReader(meta)).DecodeMulti(
		&co.Host,
		&co.Port,
		&co.User,
		&co.Hmac,
		&co.UseSSL,
		&co.Timeout,
		&co.RequestTimeout,
	)
	if err!=nil { return nil,err }
	bc,err := kinetic.NewBlockConnection(co)
	if err!=nil { return nil,err }
	bt := &Bucket{bc}
	return &netkv.Session{
		Closer: bt,
		Reader: bt,
		Writer: bt,
	},nil
}
func init() {
	netkv.Provider(loader).RegisterAs(PROVIDER)
}
