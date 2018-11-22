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
Article-cassandra hybrid store.
*/
package articlechs

import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore/chybrid"
import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore/netwire"

import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/cluster"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/cluster/runner"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/cluster/bucketsched"
import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/netsel"

import "github.com/lytics/confl"
import "github.com/gocql/gocql"
import "net"
import "io"

/*
Config data structure. To be parsed with confl.
	# Memberlist/Cluster settings
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
	# Articlestore-service.
	service {
		port 63300
	}
	# Cassandra-cluster
	cassandra {
		keyspace mydb
		hosts [
			"192.168.1.1"
			"192.168.1.2"
			"192.168.1.3"
		]
	}
	# Predefined Bucket Locations
	# Please only specify absolute paths.
	buckets [
		/path/to/bucket
		'/second/bucket'
		"/third/bucket"
		E:\bucket
		'F:\bucket'
		"G:\bucket"
	]
*/
type Cassa struct{
	Keyspace string
	Hosts []string
}
type Config struct{
	runner.Configuration
	Service runner.Bind
	Cassandra Cassa
	Buckets []string
}
func (bcfg *Config) LoadBytes(b []byte) error {
	return confl.Unmarshal(b,bcfg)
}
func (bcfg *Config) Load(r io.Reader) error {
	return confl.NewDecoder(r).Decode(bcfg)
}
func (bcfg *Config) NCluster() (*cluster.Deleg,error) {
	
	d,e := bcfg.Configuration.NCluster()
	if e!=nil { return nil,e }
	for _,buk := range bcfg.Buckets {
		openBucket(buk,d)
	}
	sel := new(netsel.NodeSelector).Init(d)
	
	sched := new(bucketsched.BucketScheduler)
	sched.D = d
	sched.Start()
	
	if bcfg.Service.Port!=0 {
		cluster := gocql.NewCluster(bcfg.Cassandra.Hosts...)
		cluster.Keyspace = bcfg.Cassandra.Keyspace
		cluster.Consistency = gocql.Quorum
		session,e := cluster.CreateSession()
		if e!=nil { return nil,e }
		
		sw := &chybrid.StoreWriter{Sched:sched,Flook:sel,Session:session,UseFastOver:true}
		sr := &chybrid.StoreReader{Bucket:sel,Session:session}
		
		addr := bcfg.Bind.Addr
		if bcfg.Service.Addr!="" {
			addr = bcfg.Service.Addr
		}
		l,e := net.ListenTCP("tcp", &net.TCPAddr{IP:net.ParseIP(addr),Port:bcfg.Service.Port})
		if e!=nil { return nil,e }
		go netwire.NewServer(sr,sw).Serve(l)
	}
	
	return d,e
}

