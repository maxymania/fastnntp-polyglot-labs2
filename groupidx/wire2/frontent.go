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

import "github.com/valyala/fastrpc"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx"
import "net"

const SniffHeader = "GIX"
const ProtocolVersion byte = 1

func network(netw string) func(addr string) (net.Conn, error) {
	return func(addr string) (net.Conn, error) { return net.Dial(netw,addr) }
}

func nclient(netw, addr string) *fastrpc.Client {
	c := new(fastrpc.Client)
	c.SniffHeader = SniffHeader
	c.ProtocolVersion = ProtocolVersion
	c.NewResponse = newResponse
	c.Addr = addr
	c.CompressType = fastrpc.CompressNone
	if netw!="tcp4" {
		c.Dial = network(netw)
	}
	return c
}

func nserver(ginr groupidx.GroupIndex) *fastrpc.Server {
	s := new(fastrpc.Server)
	s.SniffHeader = SniffHeader
	s.NewHandlerCtx = newHandler
	s.ProtocolVersion = ProtocolVersion
	s.Handler = createHandler(ginr)
	s.CompressType = fastrpc.CompressNone
	return s
}

func NewClient(netw, addr string) *Client {
	c := new(Client)
	c.Inner.cli = *nclient(netw,addr)
	return c
}

func NewServer(ginr groupidx.GroupIndex) *fastrpc.Server { return nserver(ginr) }
