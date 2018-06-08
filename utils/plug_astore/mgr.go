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

package plug_astore

import "github.com/byte-mug/goconfig"
import "github.com/hashicorp/memberlist"
import "github.com/maxymania/fastnntp-polyglot-labs2/utils/cluster/mlst"
import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore/graph"
import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore/netwire"
import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore/gnetwire"

import "github.com/valyala/fastrpc"
import "io/ioutil"
import "net"
import "fmt"
import "log"

type Service struct {
	ml *memberlist.Memberlist
	cfg *memberlist.Config
	g  *graph.Cluster
	srv *fastrpc.Server
	gsrv *fastrpc.Server
	l,gl net.Listener
	err1,err2 chan error
	
}
func (s *Service) serveSrv(l net.Listener) {
	s.err1 <- s.srv.Serve(l)
}
func (s *Service) serveGSrv(l net.Listener) {
	s.err2 <- s.gsrv.Serve(l)
}
func (s *Service) Init(n *Network,authorative string) error {
	s.err1 = make(chan error,1)
	s.err2 = make(chan error,1)
	var cfg *memberlist.Config
	switch n.MLType {
	case "local": cfg = memberlist.DefaultLocalConfig()
	case "LAN"  : cfg = memberlist.DefaultLANConfig  ()
	case "WAN"  : cfg = memberlist.DefaultWANConfig  ()
	default     : cfg = memberlist.DefaultLocalConfig()
	}
	
	g := new(graph.Cluster)
	g.LocalMeta.Port = n.N2n
	g.LocalMeta.UserPort = n.Srv
	g.Init()
	s.srv  = netwire.NewServer(g,g)
	s.gsrv = gnetwire.NewServer(g)
	s.g = g
	
	mlst.Configure(cfg,g,nil)
	if authorative!="" {
		data,err := ioutil.ReadFile(NormToNative(authorative))
		log.Printf("Authorative config %q %v",authorative,err)
		if err==nil {
			gcf := new(graph.CfgConfig)
			if goconfig.Parse(data,goconfig.CreateReflectHandler(gcf))==nil {
				g.SetConfig(gcf)
			}
		}
	}
	if n.Node!="" {
		cfg.Name = n.Node
	}
	
	gl,err := net.Listen("tcp",net.JoinHostPort(n.Addr,fmt.Sprint(n.N2n)))
	if err!=nil { return err }
	l,err := net.Listen("tcp",net.JoinHostPort(n.Addr,fmt.Sprint(n.Srv)))
	if err!=nil { gl.Close(); return err }
	
	s.gl=gl
	s.l=l
	
	cfg.BindAddr = n.Addr
	cfg.BindPort = n.Gossip
	cfg.AdvertiseAddr = n.Addr
	cfg.AdvertisePort = n.Gossip
	
	s.cfg = cfg
	
	return nil
}
func (s *Service) Instantiate(stors []Storage) {
	for i := range stors {
		stt,e := openStorage(&stors[i])
		if e!=nil { continue }
		s.g.AddBackend(stors[i].Ring,stors[i].Shard,stt)
	}
}
func (s *Service) Start(n *Network) error {
	var err error
	s.ml,err = memberlist.Create(s.cfg)
	if err!=nil { return err }
	
	if len(n.Join)>0 { s.ml.Join(n.Join) }
	
	go s.serveSrv(s.l)
	go s.serveGSrv(s.gl)
	return nil
}
func (s *Service) Wait() error {
	e1 := <- s.err1
	e2 := <- s.err2
	if e1==nil { e1=e2 }
	return e1
}

func (s *Service) Init2(config []byte) error {
	var cfg Config
	err := goconfig.Parse(config,goconfig.CreateReflectHandler(&cfg))
	if err!=nil { return err }
	err = s.Init(&cfg.Network,cfg.Authorative)
	if err!=nil { return err }
	s.Instantiate(cfg.Storage)
	err = s.Start(&cfg.Network)
	if err!=nil { return err }
	
	return nil
}


