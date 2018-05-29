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


package wire2

import "sync"
import "bytes"
import "time"
import "sync/atomic"
import "container/list"

type stream struct {
	t sync.Mutex
	inner *bytes.Buffer
	deadline time.Time
	parent *stmap
	key    uint64
	done   bool
	elem   *list.Element
}

func (s *stream) lock() *stream {
	s.t.Lock()
	return s
}

func (s *stream) unlock() {
	ch := false
	now := time.Now()
	if s.deadline.Before(now) {
		ch = s.inner!=nil
		s.inner = nil
	}
	s.t.Unlock()
	
	/*
	Eliminate the outdated entry by touching it's position in the map, causing an auto-cleanup.
	*/
	if ch && s.parent!=nil { s.parent.obtain(s.key,false) }
}

func (s *stream) obtainPacket(i *iRequest,ndl time.Time) {
	i.Reply(true,s.inner.Next(1<<26))
	if s.inner.Len()==0 && s.done {
		s.inner = nil
	} else {
		s.deadline = ndl
	}
}


func (s *stream) Close() error {
	s.done = true
	s.lock().unlock()
	return nil
}

/* These IO-Methods are used by msgpack.Encoder */
func (s *stream) Write(b []byte) (int, error) {
	if s.inner==nil { return len(b),nil }
	defer s.lock().unlock()
	if s.inner==nil { return len(b),nil }
	return s.inner.Write(b)
}
func (s *stream) WriteByte(b byte) error {
	if s.inner==nil { return nil }
	defer s.lock().unlock()
	if s.inner==nil { return nil }
	return s.inner.WriteByte(b)
}
func (s *stream) WriteString(b string) (int, error) {
	if s.inner==nil { return len(b),nil }
	defer s.lock().unlock()
	if s.inner==nil { return len(b),nil }
	return s.inner.WriteString(b)
}



type stmap struct {
	t sync.Mutex
	m map[uint64]*stream
	l *list.List
}

func (s *stmap) lock() *stmap {
	s.t.Lock()
	if s.m==nil { s.m = make(map[uint64]*stream) }
	if s.l==nil { s.l = list.New() }
	return s
}

func (s *stmap) unlock() {
	s.t.Unlock()
}

func (s *stmap) ieatOld() {
	e := s.l.Back()
	now := time.Now()
	/* Purge dead and/or outdated elements. */
	for e!=nil {
		str := e.Value.(*stream)
		e = e.Prev()
		if str.inner==nil || str.deadline.Before(now) {
			delete(s.m,str.key)
			s.l.Remove(str.elem)
		}
	}
}
func (s *stmap) obtain(k uint64, locked bool) *stream {
	defer s.lock().unlock()
	str := s.m[k]
	if str==nil { return nil }
	if str.inner==nil {
		delete(s.m,k)
		s.l.Remove(str.elem)
		str.elem = nil
		return nil
	}
	s.l.MoveToFront(str.elem)
	s.ieatOld()
	if locked { str.lock() }
	return str
}

func (s *stmap) put(k uint64,v *stream) bool {
	defer s.lock().unlock()
	return s.put_nl(k,v)
}

func (s *stmap) put_nl(k uint64,v *stream) bool {
	str := s.m[k]
	if str==nil {
		s.iput(k,v)
		return true
	}
	if str.inner==nil {
		s.iput(k,v)
		return true
	}
	return false
}

func (s *stmap) iput(k uint64,v *stream) {
	s.m[k]   = v
	v.parent = s
	v.key    = k
	v.elem   = s.l.PushFront(v)
	s.ieatOld()
}

func (s *stmap) allocate(a *uint64,ito time.Time) (uint64,*stream) {
	defer s.lock().unlock()
	str := &stream{inner:new(bytes.Buffer),deadline:ito}
start:
	k := atomic.AddUint64(a,1)
	if !s.put_nl(k,str) { goto start }
	return k,str
}


type mstmap [128]stmap
func (s *mstmap) obtain(k uint64, locked bool) *stream { return s[k&127].obtain(k,locked) }
func (s *mstmap) put(k uint64,v *stream) bool {return s[k&127].put(k,v) }
func (s *mstmap) allocate(a *uint64,ito time.Time) (uint64,*stream) {
	str := &stream{inner:new(bytes.Buffer),deadline:ito}
start:
	k := atomic.AddUint64(a,1)
	if !s.put(k,str) { goto start }
	return k,str
}


