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


package repliseq

import "github.com/maxymania/fastnntp-polyglot-labs2/grouphead"
import "github.com/maxymania/gonbase/replidb"
import "sync"

type RepliDB struct{
	Worker *replidb.SequenceWorker
}

func grow(g []int64,s int) []int64 {
	if cap(g)<s { return make([]int64,s) }
	return g[:s]
}
func getError(acts []replidb.Action) error {
	for _,a := range acts {
		if a.Error!=nil { return a.Error }
	}
	return nil
}

var chunks = sync.Pool{ New: func() interface{} { return make([]replidb.Action,0,16) } }
func allocChunk(s int) []replidb.Action {
	arr := chunks.Get().([]replidb.Action)
	if cap(arr)<s { return make([]replidb.Action,s) }
	return arr[:s]
}
func freeChunk(arc []replidb.Action) {
	for i := range arc { arc[i] = replidb.Action{} }
	chunks.Put(arc)
}

func (r *RepliDB) GroupHeadInsert(groups [][]byte, buf []int64) ([]int64, error) {
	var wg sync.WaitGroup
	
	buf = grow(buf,len(groups))
	ars := allocChunk(len(groups))
	defer freeChunk(ars)
	for i,group := range groups {
		ars[i] = replidb.Action{
			Act: replidb.Act_Allocate,
			Name: group,
			WG: &wg,
		}
		wg.Add(1)
		r.Worker.Submit(&ars[i])
	}
	wg.Wait()
	for i := range groups {
		buf[i] = int64(ars[i].Number)
	}
	err := getError(ars)
	return buf,err
}

func (r *RepliDB) GroupHeadRevert(groups [][]byte, nums []int64) error {
	var wg sync.WaitGroup
	
	ars := allocChunk(len(groups))
	defer freeChunk(ars)
	for i,group := range groups {
		ars[i] = replidb.Action{
			Act: replidb.Act_Rollback,
			Name: group,
			Number: uint64(nums[i]),
			WG: &wg,
		}
		wg.Add(1)
		r.Worker.Submit(&ars[i])
	}
	wg.Wait()
	err := getError(ars)
	return err
}

var _ grouphead.GroupHeadDB = (*RepliDB)(nil)
