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


package groupdb2

import bolt "github.com/coreos/bbolt"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx"
import "errors"

var EMissingTable = errors.New("Missing Table")

type DB struct{
	inner *bolt.DB
	name  []byte
}
func (db *DB) batch(f func( b *bolt.Bucket) error) error {
	var err2 error
	err := db.inner.Batch(func (t *bolt.Tx) error {
		bkt,err := t.CreateBucketIfNotExists(db.name)
		if err!=nil { err2 = err; return nil }
		return f(bkt)
	})
	if err==nil { err=err2 }
	return err
}
func (db *DB) view(f func( b *bolt.Bucket) error) error {
	var err2 error
	err := db.inner.View(func (t *bolt.Tx) error {
		bkt := t.Bucket(db.name)
		if bkt==nil { err2 = EMissingTable; return nil }
		return f(bkt)
	})
	if err==nil { err=err2 }
	return err
}

func NewDB(b *bolt.DB,name []byte) *DB {
	return &DB{b,name}
}

// known from "github.com/maxymania/fastnntp-polyglot"
func (db *DB) GroupHeadInsert(groups [][]byte, buf []int64) (nums []int64, err error) {
	err = db.batch(func(t *bolt.Bucket) (err error) {
		nums,err = Tx{t}.GroupHeadInsert(groups,buf)
		return
	})
	return
}
func (db *DB) GroupHeadRevert(groups [][]byte, nums []int64) error {
	return db.batch(func(t *bolt.Bucket) (err error) {
		return Tx{t}.GroupHeadRevert(groups,nums)
	})
}
func (db *DB) ArticleGroupStat(group []byte, num int64, id_buf []byte) (id []byte, ok bool) {
	db.view(func(t *bolt.Bucket) (err error) {
		id,ok = Tx{t}.ArticleGroupStat(group,num,id_buf)
		return
	})
	return
}
func (db *DB) ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	db.view(func(t *bolt.Bucket) (err error) {
		ni,id,ok = Tx{t}.ArticleGroupMove(group,i,backward,id_buf)
		return
	})
	return
}

// Newly introduced.
func (db *DB) AssignArticleToGroup(group []byte, num, exp uint64, id []byte) error {
	return db.batch(func(t *bolt.Bucket) (err error) {
		return Tx{t}.AssignArticleToGroup(group,num,exp,id)
	})
}
func (db *DB) AssignArticleToGroups(groups [][]byte, nums []int64, exp uint64, id []byte) error {
	return db.batch(func(t *bolt.Bucket) (err error) {
		return Tx{t}.AssignArticleToGroups(groups,nums,exp,id)
	})
}
func (db *DB) ListArticleGroupRaw(group []byte, first, last int64, targ func(int64, []byte)) {
	db.view(func(t *bolt.Bucket) (err error) {
		Tx{t}.ListArticleGroupRaw(group,first,last,targ)
		return
	})
}
func (db *DB) GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool) {
	db.view(func(t *bolt.Bucket) (err error) {
		number,low,high,ok = Tx{t}.GroupRealtimeQuery(group)
		return
	})
	return
}


var _ groupidx.GroupIndex = (*DB)(nil)

