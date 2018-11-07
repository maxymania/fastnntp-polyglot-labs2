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


package graph

import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "github.com/maxymania/fastnntp-polyglot-labs2/utils/cluster"
import "github.com/valyala/fastrpc"
import "sync"
import "github.com/byte-mug/golibs/msgpackx"
import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore/gnetwire"
import "net"
//import "log"

const (
	level_none uint = iota
	level_network
	level_direct
)

type ClusterMetadata struct {
	Port int `msgpack:"port"`
	UserPort int `msgpack:"userport"`
	Stores [][2]string `msgpack:"shard"`
}
func (c *ClusterMetadata) Decode(n *cluster.Node) bool {
	var s string
	b := msgpackx.Unmarshal(n.Meta,&s,c)==nil
	return b && s=="GRAPH"
}
func (c *ClusterMetadata) Encode() []byte {
	data,_ := msgpackx.Marshal("GRAPH",c)
	return data
}

type ClusterNodeRecord struct {
	Node cluster.Node
	Info ClusterMetadata
	Cli *fastrpc.Client
}
func (c *ClusterNodeRecord) Insert(n *cluster.Node) bool{
	if !c.Info.Decode(n) { return false }
	addr := net.TCPAddr{IP:n.Addr,Port:c.Info.Port}
	c.Node = *n
	c.Cli = gnetwire.NewClient("tcp",addr.String())
	return true
}
func (c *ClusterNodeRecord) Update(n *cluster.Node) bool{
	var info ClusterMetadata
	if !info.Decode(n) { return false }
	c.Info = info
	c.Node = *n
	addr := net.TCPAddr{IP:n.Addr,Port:c.Info.Port}
	c.Cli.Addr = addr.String()
	return true
}

type Cluster struct {
	lock sync.RWMutex
	culk sync.Mutex
	Records map[string]*ClusterNodeRecord
	
	Master StorageMM
	RingMM StorageMM
	RingSt *RingSet
	
	Config *CfgConfig
	
	LocalMeta ClusterMetadata
	localSet map[[2]string]bool
}
func (c *Cluster) Lookup(K1,K2 []byte) articlestore.Storage {
	c.lock.RLock(); defer c.lock.RUnlock()
	m := c.Master[string(K1)]
	if m==nil { return nil }
	s := m[string(K2)]
	if s==nil { return nil }
	return s
}
func (c *Cluster) Init() {
	c.Records = make(map[string]*ClusterNodeRecord)
	c.Master = make(StorageMM)
}
func (c *Cluster) getLocalSet() map[[2]string]bool {
	if c.localSet==nil { c.localSet = make(map[[2]string]bool) }
	return c.localSet
}
func (c *Cluster) insert(i [2]string) {
	ls := c.getLocalSet()
	if ls[i] { return }
	ls[i] = true
	c.LocalMeta.Stores = append(c.LocalMeta.Stores,i)
	
}
func (c *Cluster) AddBackend(k1,k2 string,s articlestore.Storage) {
	c.lock.Lock(); defer c.lock.Unlock()
	c.insert([2]string{k1,k2})
	c.Master.Walk(k1,k2).Set(s,level_direct)
	if r := c.RingMM.RWalk(k1,k2); r!=nil { r.Set(s,level_network) }
}
func (c *Cluster) Metadata(limit int) []byte {
	data := c.LocalMeta.Encode()
	if len(data)>limit { data = nil }
	return data
}
func (c *Cluster) integrate(cm *ClusterNodeRecord) {
	for _,pair := range cm.Info.Stores {
		var s articlestore.Storage = gnetwire.Client{cm.Cli,pair[0],pair[1]}
		c.Master.Walk(pair[0],pair[1]).Set(s,level_network)
		if r := c.RingMM.RWalk(pair[0],pair[1]); r!=nil { r.Set(s,level_network) }
	}
}
func (c *Cluster) Create(n *cluster.Node) {
	c.lock.Lock(); defer c.lock.Unlock()
	cm := new(ClusterNodeRecord)
	if !cm.Insert(n) { return }
	c.Records[n.Name] = cm
	
	c.integrate(cm)
}
func (c *Cluster) Update(n *cluster.Node) {
	c.lock.Lock(); defer c.lock.Unlock()
	cm,ok := c.Records[n.Name]
	if !ok {
		cm = new(ClusterNodeRecord)
		if !cm.Insert(n) { return }
		c.Records[n.Name] = cm
		c.integrate(cm)
		return
	}
	if cm.Update(n) {
		
		c.integrate(cm)
	}
}
func (c *Cluster) Delete(n *cluster.Node) {
	c.lock.Lock(); defer c.lock.Unlock()
	delete(c.Records,n.Name)
}
func (c *Cluster) ValidateAll(nn []cluster.Node) bool {
	var info ClusterMetadata
	for _,n := range nn {
		if !info.Decode(&n) { return false }
	}
	return true
}

func (c *Cluster) SetConfig(cfg *CfgConfig) {
	
	c.culk.Lock(); defer c.culk.Unlock()
	st := new(RingSet)
	mm := make(StorageMM)
	st.Configure(cfg,mm)
	
	c.RingSt = st
	c.RingMM = mm
	
	
	c.lock.RLock(); defer c.lock.RUnlock()
	for k1,m := range c.Master {
		for k2,s := range m {
			
			if r := mm.RWalk(k1,k2); r!=nil { r.Set(s.Storage,s.Class) }
		}
	}
}

var _ cluster.Handler = (*Cluster)(nil)
//var _ cluster.StateHandler = (*Cluster)(nil)
var _ gnetwire.MultiStorage = (*Cluster)(nil)

func (c *Cluster) StoreReadMessage(id []byte, over, head, body bool) (bufferex.Binary, error) {
	r := c.RingSt
	if r==nil { return bufferex.Binary{},articlestore.EFail{} }
	b,err := r.StoreReadMessage(id,over,head,body)
	return b,err
}
func (c *Cluster) StoreWriteMessage(id, msg []byte, expire uint64) error {
	r := c.RingSt
	if r==nil { return articlestore.EFail{} }
	err := r.StoreWriteMessage(id,msg,expire)
	return err
}


