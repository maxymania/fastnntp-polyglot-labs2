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
import "encoding/binary"
import "github.com/maxymania/gonbase/nubrin"

var (
	iTable = []byte("table")
	iIndex = []byte("index")
	iCount = []byte("count")
)

func btoi(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
func itob(i uint64) []byte {
	b := make([]byte,8)
	binary.BigEndian.PutUint64(b,i)
	return b
}

type Tx struct{
	inner *bolt.Bucket
	name  []byte
}
func (t *Tx) createGroup(group []byte) (*bolt.Bucket,error) {
	bkt,err := t.inner.CreateBucketIfNotExists(group)
	if err!=nil { return nil,err }
	_,err = bkt.CreateBucketIfNotExists(iTable)
	if err!=nil { return nil,err }
	_,err = bkt.CreateBucketIfNotExists(iIndex)
	if err!=nil { return nil,err }
	if len(bkt.Get(iCount))<8 {
		bkt.Put(iCount,itob(0))
	}
	return bkt,err
}
func (t *Tx) AssignArticleToGroup(group []byte, num, exp uint64, id []byte) error {
	bkt,err := t.createGroup(group)
	
	if err!=nil { return err }
	
	var tsi = nubrin.TSIndex{
		Index:bkt.Bucket(iIndex),
		Table:bkt.Bucket(iTable),
		Mod:60*60*24,
	}
	
	err = tsi.Insert(num,exp,id)
	if err!=nil { return err }
	
	return bkt.Put(iCount,itob(btoi(  bkt.Get(iCount)  )+1))
}
func (t *Tx) ArticleGroupStat(group []byte, num int64, id_buf []byte) ([]byte, bool) {
	bkt := t.inner.Bucket(group)
	if bkt==nil { return nil,false }
	
	var tsi = nubrin.TSIndex{
		Index:bkt.Bucket(iIndex),
		Table:bkt.Bucket(iTable),
		Mod:60*60*24,
	}
	
	e,val := tsi.Lookup(uint64(num))
	if e < current { return nil,false }
	if len(val)==0 { return nil,false }
	
	return append(id_buf[:0],val...),true
}
func (t *Tx) GroupHeadInsert(groups [][]byte, buf []int64) ([]int64, error) {
	
	if cap(buf)<len(groups) { buf = make([]int64,len(groups)) } else { buf = buf[:len(groups)] }
	
	for i,group := range groups {
		bkt,err := t.createGroup(group)
		if err!=nil { return nil,err }
		
		n,err := bkt.Bucket(iTable).NextSequence()
		
		if err!=nil { return nil,err }
		
		buf[i] = int64(n)
	}
	
	return buf,nil
}
func (t *Tx) GroupHeadRevert(groups [][]byte, nums []int64) error {
	return nil
}



