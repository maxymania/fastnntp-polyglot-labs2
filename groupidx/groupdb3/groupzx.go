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


package groupdb3

import bolt "github.com/coreos/bbolt"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx"
import "errors"

var EMissingTable = errors.New("Missing Table")

type DB struct{
	inner *bolt.DB
	name  []byte
	slogs *slogs
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
	return &DB{b,name,new(slogs)}
}


func (db *DB) GroupHeadInsert(groups [][]byte, buf []int64) (nums []int64, err error) { return nil,EUnimplemented }
func (db *DB) GroupHeadRevert(groups [][]byte, nums []int64) error { return EUnimplemented }


func (db *DB) ArticleGroupStat(group []byte, num int64, id_buf []byte) (id []byte, ok bool) {
	row := db.slogs.lookup(TableKey{Group:group,Number:uint64(num)})
	if row!=nil {
		if row.Expires < current { return }
		id = append(id_buf[:0],row.MessageId...)
		ok = true
		return
	}
	db.view(func(t *bolt.Bucket) (err error) {
		id,ok = Tx{t}.ArticleGroupStat(group,num,id_buf)
		return
	})
	return
}
func (db *DB) ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	row := db.slogs.move(group,uint64(i),backward)
	if row!=nil {
		ui := uint64(i)
		if backward { ui-- } else { ui++ }
		if row.Number==ui {
			ni = int64(row.Number)
			id = append(id_buf[:0],row.MessageId...)
			ok = true
			return
		}
	}
	db.view(func(t *bolt.Bucket) (err error) {
		ni,id,ok = Tx{t}.ArticleGroupMove(group,i,backward,id_buf)
		return
	})
	if row!=nil {
		if row.Expires < current { return }
		if ok {
			if backward && row.Number<=uint64(ni) { return }
			if (!backward) && row.Number>=uint64(ni) { return }
		}
		ni = int64(row.Number)
		id = append(id_buf[:0],row.MessageId...)
		ok = true
	}
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
	ntarg,nfin := db.slogs.wrap(group,first,last,targ)
	defer nfin()
	db.view(func(t *bolt.Bucket) (err error) {
		Tx{t}.ListArticleGroupRaw(group,first,last,ntarg)
		return
	})
}
func (db *DB) GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool) {
	stats := db.slogs.getStats(group)
	db.view(func(t *bolt.Bucket) (err error) {
		number,low,high,ok = Tx{t}.GroupRealtimeQuery(group)
		return
	})
	for _,stat := range stats {
		ok = true
		if low==0 || low<stat.Low { low = stat.Low }
		if high==0 || high>stat.High { high = stat.High }
		number += stat.Count
	}
	return
}


var _ groupidx.GroupIndex = (*DB)(nil)

