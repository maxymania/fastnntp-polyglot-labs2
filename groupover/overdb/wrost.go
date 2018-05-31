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


package overdb

import "github.com/maxymania/fastnntp-polyglot-labs2/groupover"
import bolt "github.com/coreos/bbolt"
import "errors"

var einval = errors.New("einval")

var (
	iPairs = []byte("pairs")
)

type wrstore struct{
	db     *bolt.DB
	name   []byte
}

type wrstore_batch struct{
	t *bolt.Tx
	name   []byte
}
func (w *wrstore) GroupOverviewList(targ func(group []byte, statusAndDescr []byte)) bool {
	return w.db.View(func(t *bolt.Tx) (e error) {
		bkt := t.Bucket(w.name)
		if bkt==nil { return }
		lst := bkt.Bucket(iPairs)
		if lst==nil { return }
		c := lst.Cursor()
		for k,v := c.First(); len(k)!=0; k,v = c.Next() {
			if len(v)==0 { continue }
			targ(k,v)
		}
		return
	})!=nil
}
func (w *wrstore) GroupOverviewGet(group []byte, buffer []byte) (statusAndDescr []byte) {
	w.db.View(func(t *bolt.Tx) (e error) {
		bkt := t.Bucket(w.name)
		if bkt==nil { return }
		lst := bkt.Bucket(iPairs)
		if lst==nil { return }
		statusAndDescr = append(buffer[:0],lst.Get(group)...)
		return
	})
	return
}
func (w *wrstore_batch) updateGroup(group []byte, cmd uint8, status byte, descr []byte) error {
	t := w.t
	{
		bkt,err := t.CreateBucketIfNotExists(w.name)
		if err!=nil { return err }
		lst,err := bkt.CreateBucketIfNotExists(iPairs)
		if err!=nil { return err }
		
		oentry := lst.Get(group)
		switch cmd {
		case 1:
			if len(oentry)<=1 { return lst.Put(group,[]byte{status}) }
			entry := make([]byte,len(oentry))
			copy(entry,oentry)
			entry[0] = status
			return lst.Put(group,entry)
		case 2:
			if len(oentry)>0 { status = oentry[0] }
			entry := make([]byte,len(descr)+1)
			copy(entry[1:],descr)
			entry[0] = status
			return lst.Put(group,entry)
		}
		
		return einval
	}
}
func (w *wrstore) updateGroup(group []byte, cmd uint8, status byte, descr []byte) error {
	return w.db.Batch(func(t *bolt.Tx) (e error) {
		b := wrstore_batch{t,w.name}
		return b.updateGroup(group,cmd,status,descr)
	})
}
func (w *wrstore) GroupOverviewSetStatus(group []byte, status byte) error {
	return w.updateGroup(group,1,status,nil)
}
func (w *wrstore) GroupOverviewSetDescr(group []byte, descr []byte) error {
	return w.updateGroup(group,2,0,descr)
}
func (w *wrstore) GroupOverviewBatchUpdate(targ func(groupover.GroupOverviewUpdater)error) error {
	return w.db.Update(func(t *bolt.Tx) (e error) {
		b := &wrstore_batch{t,w.name}
		return targ(b)
	})
}

func (w *wrstore_batch) GroupOverviewSetStatus(group []byte, status byte) error {
	return w.updateGroup(group,1,status,nil)
}
func (w *wrstore_batch) GroupOverviewSetDescr(group []byte, descr []byte) error {
	return w.updateGroup(group,2,0,descr)
}

var _ groupover.GroupOverview = (*wrstore)(nil)
var _ groupover.GroupOverviewUpdater = (*wrstore)(nil)
var _ groupover.GroupOverviewUpdater = (*wrstore_batch)(nil)
var _ groupover.GroupOverviewBatchUpdater = (*wrstore)(nil)
