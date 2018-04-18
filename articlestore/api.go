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

package articlestore

import "encoding/binary"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"

type EFail struct{}
func (e EFail) Error() string { return "Failed" }

type StorageR interface {
	StoreWriteMessage(id, msg []byte, expire uint64) error
}
type StorageW interface {
	StoreReadMessage(id []byte, over,head,body bool) (bufferex.Binary,error)
}

// Encode a message
func PackMessage(over, head, body []byte) (b bufferex.Binary,e error) {
	if len(over)>0xffff || len(head)>0xffff { e = EFail{}; return }
	l := len(over)+len(head)+len(body)+4
	b = bufferex.AllocBinary(l)
	ba := b.Bytes()
	binary.BigEndian.PutUint16(ba,uint16(len(over))); ba = ba[2:]
	binary.BigEndian.PutUint16(ba,uint16(len(head))); ba = ba[2:]
	copy(ba,over); ba = ba[len(over):]
	copy(ba,head); ba = ba[len(head):]
	copy(ba,body)
	return
}
// Decode a message
func UnpackMessage(msg []byte) (over,head,body []byte) {
	lo := binary.BigEndian.Uint16(msg); msg = msg[2:]
	lh := binary.BigEndian.Uint16(msg); msg = msg[2:]
	over,msg = msg[:lo],msg[lo:]
	head,msg = msg[:lh],msg[lh:]
	body = msg
	return
}


