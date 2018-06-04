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


package timefbak

import timefile "github.com/maxymania/storage-engines/timefile2"
import "io"
import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"

type Backend struct{
	store *timefile.Store
}
var _ articlestore.Storage = &Backend{}

func MakeBackend(s *timefile.Store) *Backend { return &Backend{s} }

func (b *Backend) StoreWriteMessage(id, msg []byte, expire uint64) error {
	return b.store.Insert(id,msg,expire)
}

type getter struct{
	blob bufferex.Binary
}
func (g *getter) SetValue(f io.ReaderAt, off int64, lng int32) error {
	g.blob = bufferex.AllocBinary(int(lng))
	_,e := f.ReadAt(g.blob.Bytes(),off)
	if e!=nil {
		g.blob.Free()
		g.blob = bufferex.Binary{}
	}
	return e
}

func (b *Backend) StoreReadMessage(id []byte, over,head,body bool) (bufferex.Binary,error) {
	var g getter
	err := b.store.Get(id,&g)
	return g.blob,err
}
