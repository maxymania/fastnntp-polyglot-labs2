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


package minifier

import "encoding/binary"
import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"

func nullify(b *[]byte, o bool) bool {
	if o || len(*b)==0 { return false }
	*b = nil
	return true
}

func split16(b []byte, p uint16) ([]byte,[]byte) {
	if len(b)<=int(p) { return b,nil }
	return b[:p],b[p:]
}
func UnpackMessageSafe(msg []byte) (over,head,body []byte) {
	lo := binary.BigEndian.Uint16(msg); msg = msg[2:]
	lh := binary.BigEndian.Uint16(msg); msg = msg[2:]
	over,msg = split16(msg,lo)
	head,msg = split16(msg,lh)
	body = msg
	return
}

func overhead(a,b,c []byte) int { return len(a)+len(b)+len(c)+4 }

type RWrapper struct{
	articlestore.StorageR
	Safe bool
}
func (r *RWrapper) repack(b *bufferex.Binary, over, head, body bool) {
	var bover,bhead,bbody []byte
	if r.Safe {
		bover,bhead,bbody = UnpackMessageSafe(b.Bytes())
	} else {
		bover,bhead,bbody = articlestore.UnpackMessage(b.Bytes())
	}
	before := overhead(bover,bhead,bbody)
	exch := false
	exch = exch || nullify( &bover , over )
	exch = exch || nullify( &bhead , head )
	exch = exch || nullify( &bbody , body )
	if !exch { return }
	after := overhead(bover,bhead,bbody)
	
	/* Minimum size reduction. Otherwise no reduction. */
	if (before/2) < after { return }
	
	n,e := articlestore.PackMessage(bover,bhead,bbody)
	if e!=nil { return }
	b.Free()
	*b = n
}
func (r *RWrapper) StoreReadMessage(id []byte, over, head, body bool) (bufferex.Binary, error) {
	b,err := r.StorageR.StoreReadMessage(id,over,head,body)
	if err==nil { r.repack(&b,over,head,body) }
	return b,err
}


