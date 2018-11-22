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


package netkv

import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore"
import "sync"
import "io"

type Session struct {
	Closer io.Closer
	Reader bucketstore.BucketR
	Writer bucketstore.BucketW
	WriterEx bucketstore.BucketWEx
}
func (s *Session) Close() error {
	if s.Closer==nil { return nil }
	return s.Closer.Close()
}

type Provider func(bucket, meta []byte) (*Session,error)

var providers = make(map[string]Provider)

// Used to register providers.
// Call this only during the init() phase.
func (p Provider) RegisterAs(pname string) {
	providers[pname] = p
}

type NetKVMap struct {
	mtx sync.RWMutex
	nwk map[string]*Session
	glo map[string]bool
}
func (n *NetKVMap) GetBucketList() []string {
	n.mtx.RLock(); defer n.mtx.RUnlock()
	sl := make([]string,len(n.nwk))
	for s := range n.nwk { sl = append(sl,s) }
	return sl
}
func (n *NetKVMap) Init() {
	n.nwk = make(map[string]*Session)
	n.glo = make(map[string]bool)
}
func (n *NetKVMap) Get(bucket []byte) *Session {
	n.mtx.RLock(); defer n.mtx.RUnlock()
	return n.nwk[string(bucket)]
}
func (n *NetKVMap) GetWithInfo(bucket []byte) (sess *Session,global bool) {
	n.mtx.RLock(); defer n.mtx.RUnlock()
	sess = n.nwk[string(bucket)]
	global = n.glo[string(bucket)]
	return
}
func (n *NetKVMap) IsNet(bucket []byte) (has,global bool) {
	n.mtx.RLock(); defer n.mtx.RUnlock()
	global,has = n.glo[string(bucket)]
	return
}
func (n *NetKVMap) GetLocalBuckets() (r []string) {
	n.mtx.RLock(); defer n.mtx.RUnlock()
	for k,g := range n.glo {
		if g { continue }
		r = append(r,k)
	}
	return
}
func (n *NetKVMap) put(bucket []byte,sess *Session,global bool) (reject,ret *Session) {
	n.mtx.Lock(); defer n.mtx.Unlock()
	var ok bool
	ret,ok = n.nwk[string(bucket)]
	if ok {
		reject = sess
	} else {
		sbu := string(bucket)
		n.nwk[sbu] = sess
		n.glo[sbu] = global
		ret = sess
	}
	return
}
func (n *NetKVMap) pull(bucket []byte) *Session {
	n.mtx.Lock(); defer n.mtx.Unlock()
	sess := n.nwk[string(bucket)]
	delete(n.nwk,string(bucket))
	delete(n.glo,string(bucket))
	return sess
}
func (n *NetKVMap) Offer(provider string, bucket, meta []byte,global bool) bool {
	if n.Get(bucket)!=nil { return true }
	
	p,ok := providers[provider]
	if !ok { return false }
	conn,err := p(bucket,meta)
	if err!=nil { return false }
	
	rej,_ := n.put(bucket,conn,global)
	if rej!=nil { rej.Close() }
	return true
}
func (n *NetKVMap) Remove(bucket []byte) {
	ret := n.pull(bucket)
	if ret!=nil { ret.Close() }
}

