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


package bucketmap

import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore"
import "sync"

type Bucket struct {
	Reader bucketstore.BucketR
	Writer bucketstore.BucketW
	WriterEx bucketstore.BucketWEx
}

type bucketmap map[string]*Bucket

type BucketMap struct{
	buckets bucketmap
	access sync.RWMutex
}
func (b *BucketMap) Init() {
	b.buckets = make(bucketmap)
}
func (b *BucketMap) Obtain(bucket []byte) (Bucket,bool) {
	b.access.RLock(); defer b.access.RUnlock()
	bkt,ok := b.buckets[string(bucket)]
	if (!ok) || bkt==nil { return Bucket{},false }
	return *bkt,true
}
func (b *BucketMap) Contains(bucket []byte) bool {
	b.access.RLock(); defer b.access.RUnlock()
	bkt,ok := b.buckets[string(bucket)]
	if (!ok) || bkt==nil { return false }
	return true
}
func (b *BucketMap) ListupRaw() (buckets []string) {
	b.access.RLock(); defer b.access.RUnlock()
	buckets = make([]string,0,len(b.buckets))
	for k := range b.buckets { buckets = append(buckets,k) }
	return
}
func (b *BucketMap) Listup() (buckets [][]byte) {
	b.access.RLock(); defer b.access.RUnlock()
	buckets = make([][]byte,0,len(b.buckets))
	for k := range b.buckets { buckets = append(buckets,[]byte(k)) }
	return
}
func (b *BucketMap) Add(name []byte,buck Bucket) {
	b.access.Lock(); defer b.access.Unlock()
	b.buckets[string(name)] = &buck
}
func (b *BucketMap) Remove(name []byte) {
	b.access.Lock(); defer b.access.Unlock()
	delete(b.buckets,string(name))
}
