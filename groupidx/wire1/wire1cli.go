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
import "errors"
import "net"

func toError(b bool,s string) error {
	if b { return nil }
	return errors.New(s)
}

var ENoResult = errors.New("NoResult")

type Client struct{
	Inner ssh.Conn
}
func NewClient(c net.Conn,addr,user,pwd string, o ...Option) (*Client,error) {
	cc := new(ssh.ClientConfig)
	cc.User = user
	cc.Auth = []ssh.AuthMethod{ssh.Password(pwd)}
	cc.HostKeyCallback = ssh.InsecureIgnoreHostKey()
	for _,oo := range o {
		oo.applyClient(cc)
	}
	conn,chs,reqs,err := ssh.NewClientConn(c,addr,cc)
	if err!=nil { return nil,err }
	go blackHole_Chan_do(chs)
	go blackHole_Req_do(reqs)
	return &Client{conn},nil
}

// known from "github.com/maxymania/fastnntp-polyglot"
func (c *Client) GroupHeadInsert(groups [][]byte, buf []int64) ([]int64, error) {
	var s string
	var b bool
	data,err := msgpack.Marshal(groups)
	if err!=nil { return nil,err }
	ok,data,err := c.Inner.SendRequest("wire1://GroupHeadInsert",true,data)
	if err!=nil { return nil,err }
	if !ok { return nil,ENoResult }
	err = msgpack.Unmarshal(data,&b,&s,&buf)
	if err!=nil { return nil,err }
	return buf,toError(b,s)
}

func (c *Client) GroupHeadRevert(groups [][]byte, nums []int64) error {
	var s string
	var b bool
	data,err := msgpack.Marshal(groups,nums)
	if err!=nil { return err }
	ok,data,err := c.Inner.SendRequest("wire1://GroupHeadRevert",true,data)
	if err!=nil { return err }
	if !ok { return ENoResult }
	err = msgpack.Unmarshal(data,&b,&s)
	if err!=nil { return err }
	return toError(b,s)
}

func (c *Client) ArticleGroupStat(group []byte, num int64, id_buf []byte) ([]byte, bool) {
	var b bool
	data,err := msgpack.Marshal(group,num)
	if err!=nil { return nil,false }
	ok,data,err := c.Inner.SendRequest("wire1://ArticleGroupStat",true,data)
	if err!=nil { return nil,false }
	if !ok { return nil,false }
	err = msgpack.Unmarshal(data,&b,&id_buf)
	if err!=nil { return nil,false }
	return id_buf,b
}

func (c *Client) ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, _ok bool) {
	var b bool
	data,err := msgpack.Marshal(group,i,backward)
	if err!=nil { return 0,nil,false }
	ok,data,err := c.Inner.SendRequest("wire1://ArticleGroupMove",true,data)
	if err!=nil { return 0,nil,false }
	if !ok { return 0,nil,false }
	err = msgpack.Unmarshal(data,&b,&id_buf,&i)
	if err!=nil { return 0,nil,false }
	return i,id_buf,b
}

// Newly introduced.
func (c *Client) AssignArticleToGroup(group []byte, num, exp uint64, id []byte) error {
	var s string
	var b bool
	data,err := msgpack.Marshal(group,num,exp,id)
	if err!=nil { return err }
	ok,data,err := c.Inner.SendRequest("wire1://AssignArticleToGroup",true,data)
	if err!=nil { return err }
	if !ok { return ENoResult }
	err = msgpack.Unmarshal(data,&b,&s)
	if err!=nil { return err }
	return toError(b,s)
}

func (c *Client) AssignArticleToGroups(groups [][]byte, nums []int64, exp uint64, id []byte) error {
	var s string
	var b bool
	data,err := msgpack.Marshal(groups,nums,exp,id)
	if err!=nil { return err }
	ok,data,err := c.Inner.SendRequest("wire1://AssignArticleToGroups",true,data)
	if err!=nil { return err }
	if !ok { return ENoResult }
	err = msgpack.Unmarshal(data,&b,&s)
	if err!=nil { return err }
	return toError(b,s)
}

func (c *Client) borrowBinary() (ssh.Channel,error) {
	ch,reqs,err := c.Inner.OpenChannel("wire1://Binary",nil)
	if err!=nil { return nil,err }
	defer ch.Close()
	go blackHole_Req_do(reqs)
	return ch,nil
}

func (c *Client) releaseBinary(ch ssh.Channel) {
	ch.Close()
}

func (c *Client) ListArticleGroupRaw(group []byte, first, last int64, targ func(int64, []byte)) {
	ch,err := c.borrowBinary()
	if err!=nil { return }
	defer c.releaseBinary(ch)
	
	w := smallWriters.Create(ch)
	enc := msgpack.NewEncoder(w)
	dec := msgpack.NewDecoder(ch)
	
	if err := enc.Encode("ListArticleGroupRaw",first,last,group); err==nil { return }
	if err := w.Flush(); err!=nil { return }
	
	var num int64
	var id []byte
	for {
		if err := dec.Decode(&num,&id); err!=nil { break }
		targ(num,id)
	}
}

func (c *Client) ArticleGroupList(group []byte, first, last int64, targ func(int64)) {
	ch,err := c.borrowBinary()
	if err!=nil { return }
	defer c.releaseBinary(ch)
	
	w := smallWriters.Create(ch)
	enc := msgpack.NewEncoder(w)
	dec := msgpack.NewDecoder(ch)
	
	if err := enc.Encode("ArticleGroupList",first,last,group); err==nil { return }
	if err := w.Flush(); err!=nil { return }
	
	var num int64
	for {
		if err := dec.Decode(&num); err!=nil { break }
		targ(num)
	}
}

var _ groupidx.GroupIndex = (*Client)(nil)

