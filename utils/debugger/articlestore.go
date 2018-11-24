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
A component debugger.
*/
package debugger

import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "log"
import "fmt"

type bufbin bufferex.Binary
func (b_ bufbin) String() string {
	b := bufferex.Binary(b_)
	e1 := b.Bytes()
	e2 := b.BufferAllocUnit()
	fill := ""
	if len(e1)>10 { e1,fill = e1[:10],"..." }
	return fmt.Sprintf("Bx{%x%s,%p}",e1,fill,e2)
}
type rawbin []byte
func (b rawbin) String() string {
	fill := ""
	if len(b)>10 { b,fill = b[:10],"..." }
	return fmt.Sprintf("%x%s",[]byte(b),fill)
}

type DebugStorageR struct{
	articlestore.StorageR
}
func (d DebugStorageR) StoreReadMessage(id []byte, over, head, body bool) (buf bufferex.Binary, err error) {
	buf,err = d.StorageR.StoreReadMessage(id,over,head,body)
	log.Printf("StoreReadMessage(%q,%v,%v,%v) -> (%v,%v)",id,over,head,body,bufbin(buf),err)
	return
}
type DebugStorageW struct{
	articlestore.StorageW
}
func (d DebugStorageW) StoreWriteMessage(id, msg []byte, expire uint64) error {
	err := d.StorageW.StoreWriteMessage(id,msg,expire)
	log.Printf("StoreWriteMessage(%q,%v,%v)->%v",id,rawbin(msg),expire,err)
	return err
}
