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


package netwire

import "github.com/valyala/fastrpc"
import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "time"

func has(u,v uint64) bool {
	return (u&v)!=0
}
func cond(v uint64,b bool) uint64 {
	if !b { return 0 }
	return v
}

func createHandler(SR articlestore.StorageR,SW articlestore.StorageW) func(ctx fastrpc.HandlerCtx) fastrpc.HandlerCtx {
	
	handleRequest := func(r *iRequest) {
		var over,head,body bool
		switch string(r.Type) {
		case "R":
			if SR==nil { return }
			over = has(r.Expire,1)
			head = has(r.Expire,2)
			body = has(r.Expire,4)
			r.RespondB(SR.StoreReadMessage(r.MessageId,over,head,body))
		case "W":
			if SW==nil { return }
			r.Respond(nil,SW.StoreWriteMessage(r.MessageId,r.Payload,r.Expire))
		}
	}
	
	return func(ctx fastrpc.HandlerCtx) fastrpc.HandlerCtx {
		handleRequest(&ctx.(*handlerctx).inner)
		return ctx
	}
}

type Client struct {
	Cli *fastrpc.Client
}

type ClientR Client

func (c ClientR) StoreReadMessage(id []byte, over, head, body bool) (bufferex.Binary, error) {
	req := reqPool.Get().(*request)
	defer reqPool.Put(req)
	resp := respPool.Get().(*response)
	defer respPool.Put(resp)
	
	req.inner.Type = append(req.inner.Type[:0],"R"...)
	req.inner.MessageId = append(req.inner.MessageId[:0],id...)
	req.inner.Payload = req.inner.Payload[:0]
	req.inner.Expire = cond(1,over)|cond(2,head)|cond(4,body)
	
	err := c.Cli.DoDeadline(req,resp,time.Now().Add(time.Second*5))
	if err!=nil { return bufferex.Binary{},err }
	
	return resp.inner.GetBinary(),resp.inner.GetError()
}

var _ articlestore.StorageR = ClientR{nil}

type ClientW Client

func (c ClientW) StoreWriteMessage(id, msg []byte, expire uint64) error {
	req := reqPool.Get().(*request)
	defer reqPool.Put(req)
	resp := respPool.Get().(*response)
	defer respPool.Put(resp)
	
	req.inner.Type      = append(req.inner.Type[:0],"W"...)
	req.inner.MessageId = append(req.inner.MessageId[:0],id...)
	req.inner.Payload   = append(req.inner.Payload[:0],msg...)
	req.inner.Expire    = expire
	
	err := c.Cli.DoDeadline(req,resp,time.Now().Add(time.Second*5))
	if err!=nil { return err }
	
	return resp.inner.GetError()
}

var _ articlestore.StorageW = ClientW{nil}

