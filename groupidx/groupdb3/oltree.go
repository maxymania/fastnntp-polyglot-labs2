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

import avl "github.com/emirpasic/gods/trees/avltree"
//import "github.com/emirpasic/gods/utils"
import "github.com/vmihailenco/msgpack"

import "bytes"
import "bufio"
import "sync"
import "os"
import "errors"

func bytesComparator(a, b interface{}) int {
	return bytes.Compare(a.([]byte),b.([]byte))
}

type TableKey struct{
	_msgpack struct{} `msgpack:",asArray"`
	Group  []byte
	Number uint64
}
func (t *TableKey) decode(a interface{}) {
	switch v := a.(type){
	case TableKey: *t = v
	case *TableKey: *t = *v
	default: panic("invalid key")
	}
}
func TKComparator(a, b interface{}) int {
	var A,B TableKey
	A.decode(a)
	B.decode(b)
	cmp := bytes.Compare(A.Group,B.Group)
	if cmp!=0 { return cmp }
	if A.Number<B.Number { return -1 }
	if A.Number>B.Number { return 1 }
	return 0
}
func tkCompare(A,B TableKey) int {
	cmp := bytes.Compare(A.Group,B.Group)
	if cmp!=0 { return cmp }
	if A.Number<B.Number { return -1 }
	if A.Number>B.Number { return 1 }
	return 0
}

type TableRow struct{
	_msgpack struct{} `msgpack:",asArray"`
	TableKey
	Expires   uint64
	MessageId []byte
}

type GroupStats struct{
	Count,Low,High int64
}

type ListGroupStats []GroupStats
func (l ListGroupStats) Cuts(begin,end int64) bool {
	for _,gs := range l{
		if end<=gs.Low { continue }
		if gs.High<begin { continue }
		return true
	}
	return false
}

func treeLookup(t *avl.Tree,key interface{}) *avl.Node {
	n := t.Root
	for n != nil {
		cmp := t.Comparator(key, n.Key)
		switch {
		case cmp == 0:
			return n
		case cmp < 0:
			n = n.Children[0]
		case cmp > 0:
			n = n.Children[1]
		}
	}
	return nil
}

type slog struct{
	WB   sync.Mutex
	Lock sync.RWMutex
	Enc  *msgpack.Encoder
	Buf  *bufio.Writer
	Tree *avl.Tree
	Gsix *avl.Tree
	File *os.File
	Path string
}
func mkslog(f *os.File,p string) *slog {
	buf := bufio.NewWriter(f)
	return &slog{
		Enc: msgpack.NewEncoder(buf),
		Buf: buf,
		Tree: avl.NewWith(TKComparator),
		Gsix: avl.NewWith(bytesComparator),
		File: f,
		Path: p,
	}
}
func mkslog2() *slog {
	return &slog{
		Tree: avl.NewWith(TKComparator),
		Gsix: avl.NewWith(bytesComparator),
	}
}


func (s *slog) dedupeString(b *[]byte) bool {
	s.Lock.RLock(); defer s.Lock.RUnlock()
	n := treeLookup(s.Gsix,*b)
	if n!=nil {
		*b = n.Key.([]byte)
		return true
	}
	return false
}
func (s *slog) getStats(group []byte) *GroupStats {
	if v,ok := s.Gsix.Get(group); ok { return v.(*GroupStats) }
	return nil
}
func (s *slog) mark(group []byte,i int64) {
	if v,ok := s.Gsix.Get(group); ok {
		vv := v.(*GroupStats)
		vv.Count++
		if vv.Low>i { vv.Low = i }
		if vv.High<i { vv.High = i }
	} else {
		if i==0 { i = 1 }
		s.Gsix.Put(group,&GroupStats{1,i,i})
	}
}
func (s *slog) insertMem(row *TableRow) {
	s.Lock.Lock(); defer s.Lock.Unlock()
	s.mark(row.Group,int64(row.Number))
	s.Tree.Put(&row.TableKey,row)
}
func (s *slog) insert(row *TableRow) error {
	s.WB.Lock(); defer s.WB.Unlock()
	if s.Enc==nil { return errors.New("No encoder.") }
	err := s.Enc.Encode(row)
	if err!=nil { return err }
	err = s.Buf.Flush()
	if err!=nil { return err }
	s.insertMem(row)
	return nil
}
func (s *slog) deleteIt() {
	s.File.Close()
	os.Remove(s.Path)
	s.Lock.Lock(); defer s.Lock.Unlock()
	s.Tree.Clear()
	s.Gsix.Clear()
}
func (s *slog) lookup(k interface{}) *TableRow {
	s.Lock.RLock(); defer s.Lock.RUnlock()
	v,ok := s.Tree.Get(k)
	if ok { return v.(*TableRow) }
	return nil
}
func (s *slog) floor(k interface{}) *TableRow {
	s.Lock.RLock(); defer s.Lock.RUnlock()
	n,_ := s.Tree.Floor(k)
	if n==nil { return nil }
	return n.Value.(*TableRow)
}
func (s *slog) ceiling(k interface{}) *TableRow {
	s.Lock.RLock(); defer s.Lock.RUnlock()
	n,_ := s.Tree.Ceiling(k)
	if n==nil { return nil }
	return n.Value.(*TableRow)
}

type slogs struct{
	WB    sync.Mutex
	Lock  sync.RWMutex
	Stack []*slog
}
func (s *slogs) dedupeString(b *[]byte) bool {
	s.Lock.RLock(); defer s.Lock.RUnlock()
	for _,slog := range s.Stack {
		if slog.dedupeString(b) { return true }
	}
	*b = append([]byte(nil),(*b)...)
	return true
}
func (s *slogs) insert(row *TableRow) error {
	s.Lock.RLock(); defer s.Lock.RUnlock()
	if len(s.Stack)==0 { return errors.New("stack empty") }
	s.dedupeString(&row.Group)
	return s.Stack[len(s.Stack)-1].insert(row)
}
func (s *slogs) lookup(k interface{}) (v *TableRow) {
	s.Lock.RLock(); defer s.Lock.RUnlock()
	for _,slog := range s.Stack {
		v = slog.lookup(k)
		if v!=nil { break }
	}
	return
}
func (s *slogs) floor(k interface{}) (v *TableRow) {
	var nv *TableRow
	s.Lock.RLock(); defer s.Lock.RUnlock()
	for _,slog := range s.Stack {
		nv = slog.floor(k)
		if v==nil {
			v = nv
		} else if tkCompare(v.TableKey,nv.TableKey)<0 {
			v = nv
		}
	}
	return
}
func (s *slogs) ceiling(k interface{}) (v *TableRow) {
	var nv *TableRow
	s.Lock.RLock(); defer s.Lock.RUnlock()
	for _,slog := range s.Stack {
		nv = slog.ceiling(k)
		if v==nil {
			v = nv
		} else if tkCompare(nv.TableKey,v.TableKey)<0 {
			v = nv
		}
	}
	return
}
func (s *slogs) move(group []byte,i uint64,backward bool) *TableRow {
	if backward {
		if i==0 { return nil }
		return s.floor(TableKey{Group:group,Number:i-1})
	} else {
		if i== (^uint64(0)) { return nil }
		return s.ceiling(TableKey{Group:group,Number:i+1})
	}
}
func (s *slogs) shift() {
	s.Lock.Lock(); defer s.Lock.Unlock()
	if len(s.Stack)==0 { return }
	s.Stack = s.Stack[1:]
}
func (s *slogs) push(sl *slog) {
	s.Lock.Lock(); defer s.Lock.Unlock()
	s.Stack = append(s.Stack,sl)
}
func (s *slogs) getStats(group []byte) (gs ListGroupStats) {
	s.Lock.RLock(); defer s.Lock.RUnlock()
	gs = make(ListGroupStats,0,len(s.Stack))
	for _,slog := range s.Stack {
		obj := slog.getStats(group)
		if obj!=nil { gs = append(gs,*obj) }
	}
	return
}

func (s *slogs) wrap(group []byte, first, last int64, targ func(int64, []byte)) (func(int64, []byte),func()) {
	stats := s.getStats(group)
	kb := &TableKey{Group:group}
	next := first
	iterate := func(i int64) {
		if next >= i { return }
		if !stats.Cuts(next,i) { return }
		kb.Number = uint64(next)
		row := s.ceiling(kb)
		for row.Number<uint64(i) {
			targ(int64(row.Number),row.MessageId)
			kb.Number = row.Number+1
			row = s.ceiling(kb)
		}
	}
	return func(i int64, id []byte){
		iterate(i)
		targ(i,id)
		next = i+1
	},func(){
		iterate(last+1)
	}
}

