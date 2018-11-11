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


package runner

import (
	"github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/cluster"
	"github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/kvrpc"
	"github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/selector"
	"github.com/hashicorp/memberlist"
	"github.com/lytics/confl"
	"net"
	"io"
)

type Bind struct{
	Addr string
	Port int
}

/*
Config data structure. To be parsed with confl.

	bind {
		addr '0.0.0.0'
		port 7946
	}
	advertise {
		port 7946
	}
	name its_node_name
	loc USA
	rpc {
		port 63282
	}

*/
type Configuration struct{
	Bind, Advertise Bind
	Name,Loc string
	Rpc Bind
}
func (bcfg *Configuration) LoadBytes(b []byte) error {
	return confl.Unmarshal(b,bcfg)
}
func (bcfg *Configuration) Load(r io.Reader) error {
	return confl.NewDecoder(r).Decode(bcfg)
}

func (bcfg *Configuration) NCluster() (*cluster.Deleg,error) {
	cfg := memberlist.DefaultLANConfig()
	clst := new(cluster.Deleg)
	if bcfg.Bind.Addr!="" { cfg.BindAddr = bcfg.Bind.Addr }
	if bcfg.Bind.Port!=0  { cfg.BindPort = bcfg.Bind.Port }
	
	if bcfg.Advertise.Addr!="" { cfg.AdvertiseAddr = bcfg.Advertise.Addr }
	if bcfg.Advertise.Port!=0  { cfg.AdvertisePort = bcfg.Advertise.Port }
	
	if bcfg.Name!="" { cfg.Name = bcfg.Name }
	
	clst.Self = cfg.Name
	clst.Meta = &cluster.Metadata{ Port:cfg.BindPort+1 }
	if bcfg.Loc!="" { clst.Meta.Loc = bcfg.Loc }
	addr := cfg.BindAddr
	if bcfg.Rpc.Addr!="" { addr = bcfg.Rpc.Addr }
	if bcfg.Rpc.Port!=0  { clst.Meta.Port = bcfg.Rpc.Port }
	
	clst.Init()
	cfg.Delegate = clst
	cfg.Events = clst
	cfg.Merge = clst
	cfg.Alive = clst
	
	l,e := net.ListenTCP("tcp", &net.TCPAddr{IP:net.ParseIP(addr),Port:clst.Meta.Port})
	if e!=nil { return nil,e }
	
	go kvrpc.NewServer(&selector.Selector{&clst.BM,&clst.NKV}).Serve(l)
	
	clst.ML,e = memberlist.Create(cfg)
	
	if e!=nil { return nil,e }
	return clst,nil
}


