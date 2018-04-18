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


package badgbak

import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore"
import "github.com/dgraph-io/badger"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"

type Backend struct{
	db *badger.DB
}
var _ articlestore.Storage = &Backend{}

// Simply stores this.
func (b *Backend) StoreWriteMessage(id, msg []byte, expire uint64) error {
	tx := b.db.NewTransaction(true)
	
	err1 := tx.SetEntry(&badger.Entry{Key:id,Value:msg,ExpiresAt:expire})
	err2 := tx.Commit(nil)
	if err1!=nil { return err1 }
	return err2
}

// Simply acquire this!
func (b *Backend) StoreReadMessage(id []byte, over,head,body bool) (bufferex.Binary,error) {
	tx := b.db.NewTransaction(false)
	defer tx.Discard()
	item,err := tx.Get(id)
	if err!=nil { return bufferex.Binary{},err }
	msg,err := item.Value()
	if err!=nil { return bufferex.Binary{},err }
	return bufferex.NewBinary(msg),nil
}

