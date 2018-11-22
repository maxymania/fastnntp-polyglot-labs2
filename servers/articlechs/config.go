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

import "github.com/maxymania/fastnntp-polyglot-labs2/bucketstore/cluster/runner"
import "github.com/lytics/confl"
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
	cassandra [
		"192.168.1.1"
		"192.168.1.2"
		"192.168.1.3"
		'E:\newsorage'
	]
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
type Config struct{
	runner.Configuration
	Service runner.Bind
	Cassandra []string
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
	if e!=nil {
		for _,buk := range bcfg.Buckets {
			openBucket(buk,d)
		}
	}
	return d,e
}

