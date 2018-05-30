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
import "github.com/syndtr/goleveldb/leveldb/table"

type rostore struct {
	tr *table.Reader
}

func (r *rostore) GroupOverviewList(targ func(group []byte, statusAndDescr []byte)) bool {
	c := r.tr.NewIterator(nil,nil)
	defer c.Release()
	
	if !c.First() { return true }
	
	targ(c.Key(),c.Value())
	
	for c.Next() { targ(c.Key(),c.Value()) }
	
	return true
}

func (r *rostore) GroupOverviewGet(group []byte, buffer []byte) (statusAndDescr []byte) {
	statusAndDescr,_ = r.tr.Get(group,nil)
	return
}

var _ groupover.GroupOverview = (*rostore)(nil)

