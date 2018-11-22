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

/*
Bucket Implementation based on Badger.
*/
package dkv

import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "github.com/dgraph-io/badger"

func bptr(bs []byte) (bp *byte) {
	if cap(bs)==0 { return nil }
	if len(bs)==0 { return &(bs[:1])[0] }
	return &bs[0]
}

func converte(e error) error {
	if e==badger.ErrKeyNotFound { e = bucketstore.ENotFound }
	return e
}

func OpenQuick(path string) (buck Bucket,err error) {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	//opts.ValueLogFileSize = 1<<27
	buck.DB,err = badger.Open(opts)
	return
}

type Bucket struct{
	DB *badger.DB
}

func (b Bucket) BucketGet(bucket,key []byte) (buf bufferex.Binary,err error) {
	tx := b.DB.NewTransaction(false)
	defer tx.Discard()
	i,e := tx.Get(key)
	if e!=nil { err = converte(e); return }
	bc := bufferex.AllocBinary(int(i.ValueSize()))
	nb,e := i.ValueCopy(bc.Bytes())
	
	if bptr(nb)!=bptr(bc.Bytes()) {
		if len(nb)!=len(bc.Bytes()) {
			bc.Free()
			return bufferex.NewBinary(nb),nil
		}
		copy(bc.Bytes(),nb)
	}
	return bc,nil
}
func (b Bucket) BucketPut(bucket,key,value []byte) error {
	tx := b.DB.NewTransaction(true)
	defer tx.Discard()
	err := tx.Set(key,value)
	if err!=nil { return err }
	return tx.Commit()
}
func (b Bucket) BucketPutExpire(bucket,key,value []byte,expiresAt uint64) error {
	tx := b.DB.NewTransaction(true)
	defer tx.Discard()
	err := tx.SetEntry(&badger.Entry{Key:key,Value:value,ExpiresAt:expiresAt})
	if err!=nil { return err }
	return tx.Commit()
}
func (b Bucket) BucketDelete(bucket,key []byte) error {
	tx := b.DB.NewTransaction(true)
	defer tx.Discard()
	err := tx.Delete(key)
	if err!=nil { return err }
	return tx.Commit()
}

var _ bucketstore.Bucket = (*Bucket)(nil)
var _ bucketstore.BucketWEx = (*Bucket)(nil)

