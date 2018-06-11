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
import "github.com/emirpasic/gods/utils"

import "bytes"
import "sync"

func bytesComparator(a, b interface{}) int {
	return bytes.Compare(a.([]byte),b.([]byte))
}

type memtable struct{
	s sync.RWMutex
	t *avl.Tree
}
func (m *memtable) init() *memtable {
	//m.s.Lock(); defer m.s.Unlock()
	m.t = avl.NewWith(bytesComparator)
	return m
}
func (m *memtable) get(d []byte) *memsubtab {
	m.s.RLock(); defer m.s.RUnlock()
	v,_ := m.t.Get(d)
	if v==nil { return nil }
	return v.(*memsubtab)
}
func (m *memtable) getOrCreate(d []byte) *memsubtab {
	g := m.get(d)
	if g!=nil { return g }
	m.s.Lock(); defer m.s.Unlock()
	v,_ := m.t.Get(d)
	if v!=nil { return v.(*memsubtab) }
	g = new(memsubtab).init()
	m.t.Put(append([]byte(nil),d...),g)
	return g
}

type memsubtab struct{
	s sync.RWMutex
	t *avl.Tree
}
func (m *memsubtab) init() *memsubtab {
	m.t = avl.NewWith(utils.Int64Comparator)
	return m
}
func (m *memsubtab) first() *avl.Node {
	if m==nil { return nil }
	m.s.RLock(); defer m.s.RUnlock()
	return m.t.Left()
}
func (m *memsubtab) next(r *avl.Node) *avl.Node {
	m.s.RLock(); defer m.s.RUnlock()
	return r.Next()
}
func (m *memsubtab) sweep() {
	m.s.Lock(); defer m.s.Unlock()
	nt := avl.NewWith(utils.Int64Comparator)
	for n := m.t.Left(); n!=nil ; n = n.Next() {
		if n.Value.(*mememtry).exp >= current {
			nt.Put(n.Key,n.Value)
		}
	}
	m.t = nt
}

type mememtry struct{
	exp  uint64
	data []byte
}

