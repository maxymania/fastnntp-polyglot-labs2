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
import "github.com/vmihailenco/msgpack"
import "fmt"
import "time"
import "io"

type idxExt1 interface{
	// known from "github.com/maxymania/fastnntp-polyglot"
	ArticleGroupList(group []byte, first, last int64, targ func(int64))
}

func createHandler(ginr groupidx.GroupIndex) func(ctx fastrpc.HandlerCtx) fastrpc.HandlerCtx {
	
	tmout := time.Second*5
	
	strmap := new(mstmap)
	strall := new(uint64)
	
	*strall = uint64(time.Now().Unix())
	
	LAGR := func(group []byte, first, last int64, enc *msgpack.Encoder, cls io.Closer) {
		ginr.ListArticleGroupRaw(group,first,last,func(id int64, bt []byte){
			enc.Encode(true,id,bt)
		})
		enc.Encode(false,int64(0),[]byte(nil))
		cls.Close()
	}
	
	var AGL func(group []byte, first, last int64, enc *msgpack.Encoder, cls io.Closer)
	if x,ok := ginr.(idxExt1); ok {
		AGL = func(group []byte, first, last int64, enc *msgpack.Encoder, cls io.Closer) {
			x.ArticleGroupList(group,first,last,func(id int64){
				enc.Encode(true,id)
			})
			enc.Encode(false,int64(0))
			cls.Close()
		}
	} else {
		AGL = func(group []byte, first, last int64, enc *msgpack.Encoder, cls io.Closer) {
			ginr.ListArticleGroupRaw(group,first,last,func(id int64, bt []byte){
				enc.Encode(true,id)
			})
			enc.Encode(false,int64(0))
			cls.Close()
		}
	}
	
	/* Well the indentation is totally screwed up, but this is because this routine is copied from wire1. */
	handleRequest := func(r *iRequest) {
		var group  []byte
		var num    int64
		var unum   uint64
		var exp    uint64
		var groups [][]byte
		var nums   []int64
		var backward bool
		var id     []byte
		
		switch string(r.Type) {
		case "stream://LAGR","stream://AGL":
			if !r.WantReply { break }
			var first, last int64
			err := msgpack.Unmarshal(r.Payload,&group,&first,&last)
			if err!=nil { break }
			
			n,str  := strmap.allocate(strall,time.Now().Add(tmout))
			
			data,_ := msgpack.Marshal(n)
			r.Reply(true,data)
			
			switch string(r.Type) {
			case "stream://LAGR": go LAGR(group,first,last,msgpack.NewEncoder(str),str)
			case "stream://AGL" : go AGL (group,first,last,msgpack.NewEncoder(str),str)
			}
			return
		case "stream://Pull":
			err := msgpack.Unmarshal(r.Payload,&unum)
			if err!=nil { break }
			str := strmap.obtain(unum,true)
			if str==nil { break }
			defer str.unlock()
			str.obtainPacket(r,time.Now().Add(tmout))
			return
		case "stream://Ping":
			err := msgpack.Unmarshal(r.Payload,&unum)
			if err!=nil { break }
			str := strmap.obtain(unum,true)
			if str!=nil {
				str.deadline = time.Now().Add(tmout)
				str.unlock()
				r.Reply(true,nil)
				return
			}
			break
		case "wire1://Ping":
			if !r.WantReply { break }
			r.Reply(true,r.Payload)
			return
		case "wire1://GroupHeadInsert":
			if !r.WantReply { break }
			err := msgpack.Unmarshal(r.Payload,&groups)
			if err==nil {
				nums,err = ginr.GroupHeadInsert(groups,make([]int64,len(groups)))
			}
			data,err2 := msgpack.Marshal(err==nil,fmt.Sprint(err),nums)
			if err2!=nil { break }
			r.Reply(true,data)
			return
		case "wire1://GroupHeadRevert":
			err := msgpack.Unmarshal(r.Payload,&groups,&nums)
			if err==nil {
				err = ginr.GroupHeadRevert(groups,nums)
			}
			data,err2 := msgpack.Marshal(err==nil,fmt.Sprint(err))
			if err2!=nil { break }
			if r.WantReply { r.Reply(true,data) }
			return
		case "wire1://ArticleGroupStat":
			if !r.WantReply { break }
			err := msgpack.Unmarshal(r.Payload,&group,&num)
			if err!=nil { break }
			id,ok := ginr.ArticleGroupStat(group,num,nil)
			
			data,err2 := msgpack.Marshal(ok,id)
			if err2!=nil { break }
			if r.WantReply { r.Reply(true,data) }
			return
		case "wire1://ArticleGroupMove":
			if !r.WantReply { break }
			err := msgpack.Unmarshal(r.Payload,&group,&num,&backward)
			if err!=nil { break }
			ni,id,ok := ginr.ArticleGroupMove(group,num,backward,nil)
			
			data,err2 := msgpack.Marshal(ok,ni,id)
			if err2!=nil { break }
			if r.WantReply { r.Reply(true,data) }
			return
		case "wire1://AssignArticleToGroup":
			if !r.WantReply { break }
			err := msgpack.Unmarshal(r.Payload,&group,&unum,&exp,&id)
			if err!=nil {
				err = ginr.AssignArticleToGroup(group,unum,exp,id)
			}
			data,err2 := msgpack.Marshal(err==nil,fmt.Sprint(err))
			if err2!=nil { break }
			if r.WantReply { r.Reply(true,data) }
			return
		case "wire1://AssignArticleToGroups":
			if !r.WantReply { break }
			err := msgpack.Unmarshal(r.Payload,&groups,&nums,&exp,&id)
			if err!=nil {
				err = ginr.AssignArticleToGroups(groups,nums,exp,id)
			}
			data,err2 := msgpack.Marshal(err==nil,fmt.Sprint(err))
			if err2!=nil { break }
			if r.WantReply { r.Reply(true,data) }
			return
		}
		
		if r.WantReply { r.Reply(false,nil) }
	}
	
	return func(ctx fastrpc.HandlerCtx) fastrpc.HandlerCtx {
		handleRequest(&ctx.(*handlerctx).inner)
		return ctx
	}
}



