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


package netsel

import "net"
import "errors"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/netmodel"

import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/cluster"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/selector"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/kvrpc"
import "container/list"
import "sync"

var ErrNoHost = errors.New("No such host")

type pool struct{
	sync.Mutex
	*list.List
}
func (p *pool) get() interface{} {
	p.Lock(); defer p.Unlock()
	e := p.Front()
	if e==nil { return nil }
	p.Remove(e)
	return e.Value
}
func (p *pool) put(v interface{}) {
	p.Lock(); defer p.Unlock()
	p.PushBack(v)
}

type NodeSelector struct{
	de *cluster.Deleg
	sel *selector.Selector
	
	p pool
	
	ml sync.RWMutex
	m map[string]*kvrpc.Client
}
func (n *NodeSelector) peekn(name string) *kvrpc.Client {
	n.ml.RLock(); defer n.ml.RUnlock()
	return n.m[name]
}
func (n *NodeSelector) putn(name string,c *kvrpc.Client) bool {
	n.ml.Lock(); defer n.ml.Unlock()
	if _,ok := n.m[name]; ok { return false }
	n.m[name] = c
	return true
}
func (n *NodeSelector) kickn(name string) {
	n.ml.Lock(); defer n.ml.Unlock()
	c,ok := n.m[name]
	if !ok { return }
	delete(n.m,name)
	n.p.put(c)
}
func (n *NodeSelector) newClient(name string) *kvrpc.Client {
	c,_ := n.p.get().(*kvrpc.Client)
	if c==nil {
		c = new(kvrpc.Client)
		c.Init()
		c.Cli.Dial = n.dial
	}
	c.Cli.Addr = name
	return c
}
func (n *NodeSelector) getClient(name string) *kvrpc.Client {
	for {
		if c := n.peekn(name); c!=nil { return c }
		c := n.newClient(name)
		if n.putn(name,c) { return c }
		n.p.put(c)
		continue
	}
	panic("unreachable")
}
func (n *NodeSelector) dial(name string) (net.Conn, error) {
	m := n.de.GetOne(name)
	if m==nil { return nil,ErrNoHost }
	return net.DialTCP("tcp",nil,&net.TCPAddr{IP:m.IP,Port:m.Port})
}

func (n *NodeSelector) Init(de *cluster.Deleg) *NodeSelector {
	n.de = de
	n.sel = &selector.Selector{&de.BM,&de.NKV}
	n.p.List = list.New()
	n.m = make(map[string]*kvrpc.Client)
	de.AddLeaveListener(n.kickn)
	return n
}
func (n *NodeSelector) GetHolder(bucket []byte) (srv netmodel.Server, t netmodel.BucketType) {
	/*
	We generally consult our local maps first (NKV and BM).
	
	Step 1: consult NKV
	*/
	if glo,has := n.de.NKV.IsNet(bucket) ; has {
		srv.Reader = n.sel
		srv.Writer = n.sel
		srv.WriterEx = n.sel
		if glo {
			t = netmodel.All
		} else {
			switch n.de.NM.CountNodes(bucket) {
			case 0,1: t = netmodel.One
			default: t = netmodel.Some
			}
		}
		return
	}
	/*
	Step 2: consult BM
	*/
	if n.de.BM.Contains(bucket) {
		srv.Reader = n.sel
		srv.Writer = n.sel
		srv.WriterEx = n.sel
		switch n.de.NM.CountNodes(bucket) {
		case 0,1: t = netmodel.One
		default: t = netmodel.Some
		}
		return
	}
	/*
	If we don't have the bucket, it is almost guaranteed, that we won't be
	one of the nodes, sharing this bucket.
	*/
	nodes := n.de.NM.NodesB(bucket)
	switch len(nodes) {
	case 0: t = netmodel.None; return
	case 1: t = netmodel.One
	default: t = netmodel.Some
	}
	elems := n.de.GetAll(nodes,make([]*cluster.NodeMetadata,0,len(nodes)))
	n.de.SortNodesDistance(elems)
	{
		cli := n.getClient(elems[0].Name)
		srv.Reader = cli
		srv.Writer = cli
		srv.WriterEx = cli
	}
	return
}
func (n *NodeSelector) oldGetHolder(bucket []byte) (srv netmodel.Server, t netmodel.BucketType) {
	/*
	We generally consult our local maps first (NKV and BM).
	
	Step 1: consult NKV
	*/
	if glo,has := n.de.NKV.IsNet(bucket) ; has {
		srv.Reader = n.sel
		srv.Writer = n.sel
		srv.WriterEx = n.sel
		if glo {
			t = netmodel.All
		} else {
			switch n.de.NM.CountNodes(bucket) {
			case 0,1: t = netmodel.One
			default: t = netmodel.Some
			}
		}
		return
	}
	/*
	Step 2: consult BM
	*/
	if n.de.BM.Contains(bucket) {
		srv.Reader = n.sel
		srv.Writer = n.sel
		srv.WriterEx = n.sel
		switch n.de.NM.CountNodes(bucket) {
		case 0,1: t = netmodel.One
		default: t = netmodel.Some
		}
		return
	}
	/*
	If we don't have the bucket, it is almost guaranteed, that we won't be
	one of the nodes, sharing this bucket.
	*/
	nodes := n.de.NM.NodesB(bucket)
	switch len(nodes) {
	case 0: t = netmodel.None; return
	case 1: t = netmodel.One
	default: t = netmodel.Some
	}
	elems := n.de.GetAll(nodes,make([]*cluster.NodeMetadata,0,len(nodes)))
	n.de.SortNodesDistance(elems)
	{
		cli := n.getClient(elems[0].Name)
		srv.Reader = cli
		srv.Writer = cli
		srv.WriterEx = cli
	}
	return
}

