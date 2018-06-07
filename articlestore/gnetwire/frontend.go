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


package gnetwire

import "github.com/valyala/fastrpc"
import "net"

const SniffHeader = "shardSTORE"
const ProtocolVersion byte = 1

func network(netw string) func(addr string) (net.Conn, error) {
	return func(addr string) (net.Conn, error) { return net.Dial(netw,addr) }
}

func getDial(netw string) func(addr string) (net.Conn, error) {
	if netw=="tcp4" { return nil }
	return network(netw)
}

func nclient(addr string,dial func(addr string) (net.Conn, error)) *fastrpc.Client {
	c := new(fastrpc.Client)
	c.SniffHeader     = SniffHeader
	c.ProtocolVersion = ProtocolVersion
	c.CompressType    = fastrpc.CompressNone
	/*----------------------------------------------------*/
	c.NewResponse     = newResponse
	c.Addr            = addr
	c.Dial            = dial
	return c
}

func nserver(MS MultiStorage) *fastrpc.Server {
	s := new(fastrpc.Server)
	s.SniffHeader     = SniffHeader
	s.ProtocolVersion = ProtocolVersion
	s.CompressType    = fastrpc.CompressNone
	/*----------------------------------------------------*/
	s.NewHandlerCtx   = newHandler
	s.Handler         = createHandler(MS)
	return s
}

func NewClient(netw, addr string) *fastrpc.Client { return nclient(addr,getDial(netw)) }

func NewClientWithDial(addr string,dial func(addr string) (net.Conn, error)) *fastrpc.Client { return nclient(addr,dial) }

func NewServer(MS MultiStorage) *fastrpc.Server { return nserver(MS) }

