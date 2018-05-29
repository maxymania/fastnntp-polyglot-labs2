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
import "github.com/maxymania/gonbase/nubrin"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx"

var (
	iTable = []byte("table")
	iIndex = []byte("index")
	iCount = []byte("count")
	
	iFree  = []byte("free")
)

type Tx struct{
	inner *bolt.Bucket
}
func MakeTx(bkt *bolt.Bucket) *Tx {
	return &Tx{inner:bkt}
}

func (t Tx) createGroup(group []byte) (*bolt.Bucket,error) {
	bkt,err := t.inner.CreateBucketIfNotExists(group)
	if err!=nil { return nil,err }
	_,err = bkt.CreateBucketIfNotExists(iTable)
	if err!=nil { return nil,err }
	_,err = bkt.CreateBucketIfNotExists(iIndex)
	if err!=nil { return nil,err }
	if len(bkt.Get(iCount))<8 {
		err = bkt.Put(iCount,nubrin.Encode(0))
	}
	if err!=nil { return nil,err }
	_,err = bkt.CreateBucketIfNotExists(iFree)
	if err!=nil { return nil,err }
	return bkt,err
}

func (t Tx) reserve(group []byte) (uint64,error) {
	bkt,err := t.createGroup(group)
	if err!=nil { return 0,err }
	f := bkt.Bucket(iFree)
	
	/* If there any entry within the Free-Set, extract and delete the lowest one. */
	{
		c := f.Cursor()
		k,_ := c.First()
		if len(k)!=0 {
			err := c.Delete()
			if err!=nil { return 0,err }
			return nubrin.Decode(k),nil
		}
	}
	
	/* Otherwise get the next auto-increment. */
	n,err := f.NextSequence()
	if err!=nil { return 0,err }
	/* We want to start with 1, not with 0. */
	if n==0 {
		n,err = f.NextSequence()
		if err!=nil { return 0,err }
	}
	
	return n,nil
}

func (t Tx) AssignArticleToGroup(group []byte, num, exp uint64, id []byte) error {
	bkt,err := t.createGroup(group)
	
	if err!=nil { return err }
	
	var tsi = nubrin.TSIndex{
		Index:bkt.Bucket(iIndex),
		Table:bkt.Bucket(iTable),
		Mod:60*60*24,
	}
	
	err = tsi.Insert(num,exp,id)
	if err!=nil { return err }
	
	return bkt.Put(iCount,nubrin.Encode(nubrin.Decode(  bkt.Get(iCount)  )+1))
}

func (t Tx) AssignArticleToGroups(groups [][]byte, nums []int64, exp uint64, id []byte) error {
	for i,group := range groups {
		if err := t.AssignArticleToGroup(group,uint64(nums[i]),exp,id); err!=nil { return err }
	}
	return nil
}

func (t Tx) ArticleGroupStat(group []byte, num int64, id_buf []byte) ([]byte, bool) {
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

func (t Tx) GroupHeadInsert(groups [][]byte, buf []int64) ([]int64, error) {
	
	if cap(buf)<len(groups) { buf = make([]int64,len(groups)) } else { buf = buf[:len(groups)] }
	
	for i,group := range groups {
		n,err := t.reserve(group)
		
		if err!=nil { return nil,err }
		
		buf[i] = int64(n)
	}
	
	return buf,nil
}

func (t Tx) GroupHeadRevert(groups [][]byte, nums []int64) error {
	for i,group := range groups {
		bkt,err := t.createGroup(group)
		
		if err!=nil { return err }
		
		/* We put the number back into the Free-Set. */
		err = bkt.Bucket(iFree).Put(nubrin.Encode(uint64(nums[i])),iFree)
		
		if err!=nil { return err }
	}
	
	return nil
}

func (t Tx) ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	bkt := t.inner.Bucket(group)
	if bkt==nil { return }
	c := bkt.Bucket(iTable).Cursor()
	
	k,v := c.Seek(nubrin.Encode(uint64(i)))
	
	ni = int64(nubrin.Decode(k))
	
	if backward {
		k,v = c.Prev()
		ni = int64(nubrin.Decode(k))
	} else if ni==i {
		k,v = c.Next()
		ni = int64(nubrin.Decode(k))
	}
	
	if len(k)==0 { return }
	if ni==0 { return }
	
	ee,mid := nubrin.SplitOffSecond(v)
	if len(mid)==0 { return }
	if nubrin.Decode(ee) < current { return }
	
	id = append(id_buf[:0],mid...)
	return
}

func (t Tx) ListArticleGroupRaw(group []byte, first, last int64, targ func(int64, []byte)) {
	bkt := t.inner.Bucket(group)
	if bkt==nil { return }
	c := bkt.Bucket(iTable).Cursor()
	
	for k,v := c.Seek(nubrin.Encode(uint64(first))); len(k)!=0; k,v = c.Next() {
		i := int64(nubrin.Decode(k))
		if i>last { break }
		ee,id := nubrin.SplitOffSecond(v)
		if nubrin.Decode(ee) >= current { targ(i,id) }
	}
}

var _ groupidx.GroupIndex = (*Tx)(nil)

