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

import "github.com/byte-mug/goconfig/datatypes"

/*
storage timefile {
	max-size: 16<<40
	max-files: 1<<10
	max-day-offset: 15
	location: 'F:/data/'
	ring:  text
	shard: text.1
}
network {
	ip-addr: 192.168.1.42
	node: test123
	
	gossip-type: local
	gossip-port: 7001
	n2n-port: 7002
	srv-port: 7003
	addr: ':9999'
}
authorative: 'F:/config/cluster.cfg'
*/

type Storage struct {
	Type         string           `inn:"$storage"`
	MaxSize      datatypes.Number `inn:"$max-size"`
	MaxFiles     datatypes.Number `inn:"$max-files"`
	MaxDayOffset int              `inn:"$max-day-offset"`
	Location     string           `inn:"$location"`
	Ring         string           `inn:"$ring"`
	Shard        string           `inn:"$shard"`
}
type Network struct {
	Addr string `inn:"$ip-addr"`
	Node string `inn:"$node"`
	
	MLType string `inn:"$gossip-type"`
	Gossip int  `inn:"$gossip-port"`
	N2n    int  `inn:"$n2n-port"`
	Srv    int  `inn:"$srv-port"`
	
	Join []string `inn:"@join"`
}

type Config struct {
	Storage  []Storage  `inn:"@storage"`
	Network    Network  `inn:"$network"`
	Authorative string  `inn:"$authorative"`
}


