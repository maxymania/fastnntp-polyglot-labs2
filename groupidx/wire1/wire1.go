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


package wire1

import "golang.org/x/crypto/ssh"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx"
import "github.com/vmihailenco/msgpack"
import "github.com/byte-mug/golibs/msgpackx"
import "fmt"
import "net"

type idxExt1 interface{
	// known from "github.com/maxymania/fastnntp-polyglot"
	ArticleGroupList(group []byte, first, last int64, targ func(int64))
}

type WireHandler struct{
	_extensible struct{}
	Inner groupidx.GroupIndex
}

func (wh *WireHandler) handle2(ch ssh.Channel, reqs <-chan *ssh.Request) {
	go blackHole_Req_do(reqs)
	defer ch.Close()
	w := largeWriters.Create(ch)
	enc := msgpack.NewEncoder(w)
	dec := msgpack.NewDecoder(ch)
	
	var first, last int64
	var group []byte
	var line  []byte
	for {
		err := dec.Decode(&line)
		if err!=nil { return }
		switch string(line) {
		case "Quit": return
		case "ListArticleGroupRaw":
			err = dec.DecodeMulti(&first,&last,&group)
			if err!=nil { return }
			wh.Inner.ListArticleGroupRaw(group,first,last,func(num int64, id []byte){
				enc.EncodeMulti(true,num,id)
			})
			enc.EncodeMulti(false,0,[]byte(nil))
			w.Flush()
		case "ArticleGroupList":
			err = dec.DecodeMulti(&first,&last,&group)
			if err!=nil { return }
			if obj,ok := wh.Inner.(idxExt1); ok {
				obj.ArticleGroupList(group,first,last,func(num int64){
					enc.EncodeMulti(true,num)
				})
			} else {
				wh.Inner.ListArticleGroupRaw(group,first,last,func(num int64, id []byte){
					enc.EncodeMulti(true,num)
				})
			}
			enc.EncodeMulti(false,0)
			w.Flush()
		default: return
		}
	}
}

func (wh *WireHandler) handleRequest(r *ssh.Request) {
	var group  []byte
	var num    int64
	var unum   uint64
	var exp    uint64
	var groups [][]byte
	var nums   []int64
	var backward bool
	var id     []byte
	
	switch r.Type {
	case "wire1://Ping":
		if !r.WantReply { break }
		r.Reply(true,r.Payload)
		return
	case "wire1://GroupHeadInsert":
		if !r.WantReply { break }
		err := msgpackx.Unmarshal(r.Payload,&groups)
		if err==nil {
			nums,err = wh.Inner.GroupHeadInsert(groups,make([]int64,len(groups)))
		}
		data,err2 := msgpackx.Marshal(err==nil,fmt.Sprint(err),nums)
		if err2!=nil { break }
		r.Reply(true,data)
		return
	case "wire1://GroupHeadRevert":
		err := msgpackx.Unmarshal(r.Payload,&groups,&nums)
		if err==nil {
			err = wh.Inner.GroupHeadRevert(groups,nums)
		}
		data,err2 := msgpackx.Marshal(err==nil,fmt.Sprint(err))
		if err2!=nil { break }
		if r.WantReply { r.Reply(true,data) }
		return
	case "wire1://ArticleGroupStat":
		if !r.WantReply { break }
		err := msgpackx.Unmarshal(r.Payload,&group,&num)
		if err!=nil { break }
		id,ok := wh.Inner.ArticleGroupStat(group,num,nil)
		
		data,err2 := msgpackx.Marshal(ok,id)
		if err2!=nil { break }
		if r.WantReply { r.Reply(true,data) }
		return
	case "wire1://ArticleGroupMove":
		if !r.WantReply { break }
		err := msgpackx.Unmarshal(r.Payload,&group,&num,&backward)
		if err!=nil { break }
		ni,id,ok := wh.Inner.ArticleGroupMove(group,num,backward,nil)
		
		data,err2 := msgpackx.Marshal(ok,ni,id)
		if err2!=nil { break }
		if r.WantReply { r.Reply(true,data) }
		return
	case "wire1://AssignArticleToGroup":
		if !r.WantReply { break }
		err := msgpackx.Unmarshal(r.Payload,&group,&unum,&exp,&id)
		if err!=nil {
			err = wh.Inner.AssignArticleToGroup(group,unum,exp,id)
		}
		data,err2 := msgpackx.Marshal(err==nil,fmt.Sprint(err))
		if err2!=nil { break }
		if r.WantReply { r.Reply(true,data) }
		return
	case "wire1://AssignArticleToGroups":
		if !r.WantReply { break }
		err := msgpackx.Unmarshal(r.Payload,&groups,&nums,&exp,&id)
		if err!=nil {
			err = wh.Inner.AssignArticleToGroups(groups,nums,exp,id)
		}
		data,err2 := msgpackx.Marshal(err==nil,fmt.Sprint(err))
		if err2!=nil { break }
		if r.WantReply { r.Reply(true,data) }
		return
	case "wire1://GroupRealtimeQuery":
		if !r.WantReply { break }
		number,low,high,ok := wh.Inner.GroupRealtimeQuery(r.Payload)
		data,err := msgpackx.Marshal(number,low,high,ok)
		if err!=nil { break }
		if r.WantReply { r.Reply(true,data) }
		return
	}
	
	if r.WantReply { r.Reply(false,nil) }
}

func (wh *WireHandler) HandleConnection(conn *ssh.ServerConn, chs <-chan ssh.NewChannel, reqs <-chan *ssh.Request) {
	//go blackHole_Req_do(reqs)
	//go blackHole_Chan_do(chs)
	for {
		select {
		case nch := <- chs:
			switch nch.ChannelType() {
			case "wire1://Binary":
				sch,srqs,err := nch.Accept()
				if err==nil { go wh.handle2(sch,srqs) }
			default:
				nch.Reject(ssh.UnknownChannelType,"unknown")
			}
		case req := <- reqs:
			wh.handleRequest(req)
		}
	}
}

type RawHandler struct{
	WireHandler
	Config ssh.ServerConfig
}

func (r *RawHandler) Handle(conn net.Conn) error {
	server,ch,req,err := ssh.NewServerConn(conn,&r.Config)
	if err!=nil { return err }
	r.HandleConnection(server,ch,req)
	return nil
}

func (r *RawHandler) SetCompatibleCiphers() {
	r.Config.Ciphers = []string{
		"aes128-ctr",
		"aes192-ctr",
		"aes256-ctr",
		
		"arcfour128",
		"arcfour256",
		"arcfour",
		
		"chacha20-poly1305@openssh.com",
		"aes128-gcm@openssh.com",
		"aes128-cbc",
		"3des-cbc",
	}
}

func (r *RawHandler) SetSafeCiphers() {
	r.Config.Ciphers = []string{
		"aes128-ctr",
		"aes192-ctr",
		"aes256-ctr",
		"chacha20-poly1305@openssh.com",
		"aes128-gcm@openssh.com",
		"aes128-cbc",
	}
}
func (r *RawHandler) SetCompatibleMac() {
	r.Config.MACs = []string{ "hmac-sha2-256-etm@openssh.com", "hmac-sha2-256", "hmac-sha1", "hmac-sha1-96", }
}

