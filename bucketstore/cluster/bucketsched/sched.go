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
Bucket-Scheduler.
*/
package bucketsched

import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/cluster"
import "math/rand"
import "time"
import "sync"

/*
Thread-safety: Single Writer, Multiple Reader
*/
type bucketList struct {
	o sync.Once
	list [][]byte
	index map[string]int
	exist map[string]uint
}
func (s *bucketList) NextBucket() ([]byte, bool) {
	bkts := s.list
	switch len(bkts) {
	case 0: return nil,false
	case 1: return bkts[0],true
	}
	return bkts[rand.Intn(len(bkts))],true
}
func (s *bucketList) add(v string) {
	if _,ok := s.index[v]; ok { return }
	l := len(s.list)
	s.index[v] = l
	s.list = append(s.list,[]byte(v))
}
func (s *bucketList) remove(v string) {
	i,ok := s.index[v]
	if !ok { return }
	l := len(s.list)-1
	delete(s.index,v)
	if i!=l {
		o := s.list[l]
		s.list[i] = o
		s.index[string(o)]=i
	}
	s.list = s.list[:l]
}
func (s *bucketList) initialize() {
	s.index = make(map[string]int)
	s.exist = make(map[string]uint)
}
func (s *bucketList) update(lists [][]string) {
	s.o.Do(s.initialize)
	for _,list := range lists {
		for _,elem := range list {
			if len(elem)==0 { continue }
			s.exist[elem]++
		}
	}
	for _,elem := range s.list {
		s.exist[string(elem)]--
	}
	for elem,rc := range s.exist {
		if rc==0 {
			s.remove(elem)
			delete(s.exist,elem)
		} else {
			s.add(elem)
			s.exist[elem] = 1
		}
	}
}

type BucketScheduler struct {
	_willChange struct{}
	bucketList
	D *cluster.Deleg
}
func (s *BucketScheduler) Start() {
	go s.perform()
}
func (s *BucketScheduler) perform() {
	t := time.Tick(time.Second)
	for {
		<- t
		d := s.D
		if d==nil { return }
		bkts1 := s.D.NM.GetBucketList()
		bkts2 := s.D.NKV.GetBucketList()
		s.update([][]string{bkts1,bkts2})
	}
}

