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
import "github.com/vmihailenco/msgpack"
import "bufio"
import "sync"
import "time"
import "bytes"
import "io"

type request struct{
	inner iRequest
	encs mpwp
	enc *msgpack.Encoder
}
func (h *request) wreset(m mpwi) {
	h.encs.mpwi = m
}
func (h *request) WriteRequest(bw *bufio.Writer) error {
	h.wreset(bw)
	defer h.wreset(empty_)
	if h.enc==nil { h.enc = msgpack.NewEncoder(&h.encs) }
	return h.inner.EncodeReq(h.enc)
}

type response struct{
	inner iRequest
	dec *msgpack.Decoder
}
func (r *response) ReadResponse(br *bufio.Reader) error {
	if r.dec==nil {
		r.dec = msgpack.NewDecoder(br)
	} else {
		r.dec.Reset(br)
	}
	defer r.dec.Reset(empty_)
	return r.inner.DecodeResp(r.dec)
}

var _ fastrpc.ResponseReader = (*response)(nil)

var reqPool = sync.Pool{ New:func() interface{} { return new(request) } }
var respPool = sync.Pool{ New:func() interface{} { return new(response) } }

func newResponse() fastrpc.ResponseReader { return respPool.Get().(fastrpc.ResponseReader) }

type client struct {
	cli fastrpc.Client
}
func (c client) SendRequest(name string, wantReply bool, payload []byte) (bool, []byte, error) {
	req := reqPool.Get().(*request)
	defer reqPool.Put(req)
	resp := respPool.Get().(*response)
	defer respPool.Put(resp)
	req.inner.Type = append(req.inner.Type[:0],name...)
	req.inner.WantReply = wantReply
	req.inner.Payload = append(req.inner.Payload[:0],payload...)
	
	err := c.cli.DoDeadline(req,resp,time.Now().Add(time.Second*5))
	
	return resp.inner.ok,append([]byte(nil),resp.inner.reply...),err
}
func (c client) SendRequest2(name string, wantReply bool, payload []byte) (bool, []byte, func(), error) {
	req := reqPool.Get().(*request)
	defer reqPool.Put(req)
	resp := respPool.Get().(*response)
	free := func() { respPool.Put(resp) }
	req.inner.Type = append(req.inner.Type[:0],name...)
	req.inner.WantReply = wantReply
	req.inner.Payload = append(req.inner.Payload[:0],payload...)
	
	err := c.cli.DoDeadline(req,resp,time.Now().Add(time.Second*5))
	
	return resp.inner.ok,append([]byte(nil),resp.inner.reply...),free,err
}
func (c client) sendRequest3(name string, wantReply bool, payload []byte) (*response, error) {
	req := reqPool.Get().(*request)
	defer reqPool.Put(req)
	resp := respPool.Get().(*response)
	req.inner.Type = append(req.inner.Type[:0],name...)
	req.inner.WantReply = wantReply
	req.inner.Payload = append(req.inner.Payload[:0],payload...)
	
	err := c.cli.DoDeadline(req,resp,time.Now().Add(time.Second*5))
	
	return resp,err
}

func emptyfree (req fastrpc.RequestWriter){}

type clientStream struct {
	cli fastrpc.Client
	rpl []byte
	cls chan struct{}
	r bytes.Reader
}
func (cS *clientStream) checkClosed() bool{
	select {
	case <- cS.cls: return true
	default: return false
	}
	return false
}
func (cS *clientStream) keepalive() {
	req := reqPool.Get().(*request)
	req.inner.Type = append(req.inner.Type,"stream://Ping"...)
	req.inner.Payload = append(req.inner.Payload,cS.rpl...)
	req.inner.WantReply = true
	tickr := time.NewTicker(time.Second)
	defer tickr.Stop()
	for {
		select {
		case <- tickr.C:
			cS.cli.SendNowait(req,emptyfree)
		case <- cS.cls:
			break
		}
	}
}
func (cS *clientStream) closeit() (int,error) {
	close(cS.cls)
	return 0,io.EOF
}
func (cS *clientStream) consume() {
	if cS.checkClosed() { return }
	if cS.r.Len()!=0 { cS.r.Reset(nil) }
	for {
		x,err := client{cS.cli}.sendRequest3("stream://Pull",true,cS.rpl)
		bad := (err!=nil) || (!x.inner.ok)
		respPool.Put(x)
		if bad { close(cS.cls) ; return }
	}
}

func (cS *clientStream) Read(b []byte) (int, error) {
	if cS.checkClosed() { return 0,io.EOF }
	for cS.r.Len()==0 {
		x,err := client{cS.cli}.sendRequest3("stream://Pull",true,cS.rpl)
		defer respPool.Put(x)
		if err!=nil { return cS.closeit() }
		if !x.inner.ok { return cS.closeit() }
		cS.r.Reset(x.inner.reply)
		x.inner.reply = nil
		if cS.r.Len()==0 {
			time.Sleep(time.Second/10)
		}
	}
	return cS.r.Read(b)
}
func (cS *clientStream) ReadByte() (byte, error) {
	if cS.checkClosed() { return 0,io.EOF }
	for cS.r.Len()==0 {
		x,err := client{cS.cli}.sendRequest3("stream://Pull",true,cS.rpl)
		defer respPool.Put(x)
		if err!=nil { close(cS.cls); return 0,io.EOF }
		if !x.inner.ok { close(cS.cls); return 0,io.EOF }//*
		cS.r.Reset(x.inner.reply)
		x.inner.reply = nil
		if cS.r.Len()==0 {
			time.Sleep(time.Second/10)
		}
	}
	return cS.r.ReadByte()
}
func (cS *clientStream) UnreadByte() error { return cS.r.UnreadByte() }

func (c client) openStream(name string, wantReply bool, payload []byte) (*clientStream, error) {
	req := reqPool.Get().(*request)
	defer reqPool.Put(req)
	resp := respPool.Get().(*response)
	defer respPool.Put(resp)
	req.inner.Type = append(req.inner.Type[:0],name...)
	req.inner.WantReply = wantReply
	req.inner.Payload = append(req.inner.Payload[:0],payload...)
	
	err := c.cli.DoDeadline(req,resp,time.Now().Add(time.Second*5))
	
	if err!=nil { return nil,err }
	
	if !resp.inner.ok { return nil,ENoResult }
	
	s := &clientStream{
		cli:  c.cli,
		rpl:  append([]byte(nil),resp.inner.reply...),
		cls:  make(chan struct{}),
		r:    *bytes.NewReader(nil),
	}
	go s.keepalive()
	
	return s,nil
}

