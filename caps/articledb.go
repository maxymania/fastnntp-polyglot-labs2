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


package caps

import "github.com/byte-mug/fastnntp/posting"
import "github.com/vmihailenco/msgpack"
import "github.com/maxymania/fastnntp-polyglot"
import "github.com/maxymania/fastnntp-polyglot/policies"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx"
import "github.com/maxymania/fastnntp-polyglot-labs2/articlestore"


func flattenP(o *newspolyglot.ArticleOverview) []interface{} {
	return []interface{}{ &o.Subject,&o.From,&o.Date,&o.MsgId,&o.Refs,&o.Bytes,&o.Lines }
}
func flatten2V(o *posting.HeadInfo,b,l int64) []interface{} {
	return []interface{}{ o.Subject, o.From, o.Date, o.MessageId, o.References, b, l }
}


type ArticleGL interface {
	ArticleGroupList(group []byte, first, last int64, targ func(int64))
}

type wrapperGL struct{
	groupidx.GroupIndex
}

func (a wrapperGL) ArticleGroupList(group []byte, first, last int64, targ func(int64)) {
	a.ListArticleGroupRaw(group, first, last, func(ni int64, id []byte) { targ(ni) })
}

func NewArticleGL(w groupidx.GroupIndex) ArticleGL {
	if gl,ok := w.(ArticleGL); ok { return gl }
	return wrapperGL{w}
}

type ArticleDB struct {
	groupidx.GroupIndex
	ArticleGL
	articlestore.StorageR
	articlestore.StorageW
	Policy policies.PostingPolicy
}

/* ------------------------------------------------------------------------ */

func (a *ArticleDB) ArticleGroupGet(group []byte, num int64, head, body bool, id_buf []byte) ([]byte, *newspolyglot.ArticleObject) {
	id,ok := a.ArticleGroupStat(group,num,id_buf)
	if !ok { return nil,nil }
	buf,err := a.StoreReadMessage(id,false,head,body)
	if err!=nil { return nil,nil }
	defer buf.Free()
	_,hd,bd := articlestore.UnpackMessage(buf.Bytes())
	ao := newspolyglot.AcquireArticleObject()
	if head {
		ao.Bufs[0],ao.Head,err = zdecode(hd)
		if err!=nil { return nil,nil }
	}
	if body {
		ao.Bufs[1],ao.Body,err = zdecode(bd)
		if err!=nil { return nil,nil }
	}
	return id,ao
}
func (a *ArticleDB) ArticleGroupOverview(group []byte, first, last int64, targ func(int64, *newspolyglot.ArticleOverview)) {
	var over newspolyglot.ArticleOverview
	r := flattenP(&over)
	
	a.ListArticleGroupRaw(group,first,last,func(ni int64, id []byte){
		buf,err := a.StoreReadMessage(id,true,false,false)
		defer buf.Free()
		if err!=nil { return }
		po,_,_ := articlestore.UnpackMessage(buf.Bytes())
		err = zunmarshal(po,r...)
		if err!=nil { return }
		targ(ni,&over)
	})
}

/* ------------------------------------------------------------------------ */

func (a *ArticleDB) ArticleDirectStat(id []byte) bool {
	buf,err := a.StoreReadMessage(id,true,false,false)
	if err!=nil { return false }
	buf.Free()
	return true
}
func (a *ArticleDB) ArticleDirectGet(id []byte, head, body bool) *newspolyglot.ArticleObject {
	buf,err := a.StoreReadMessage(id,false,head,body)
	defer buf.Free()
	if err!=nil { return nil }
	_,hd,bd := articlestore.UnpackMessage(buf.Bytes())
	ao := newspolyglot.AcquireArticleObject()
	if head {
		ao.Bufs[0],ao.Head,err = zdecode(hd)
		if err!=nil { return nil }
	}
	if body {
		ao.Bufs[1],ao.Body,err = zdecode(bd)
		if err!=nil { return nil }
	}
	return ao
}
func (a *ArticleDB) ArticleDirectOverview(id []byte) *newspolyglot.ArticleOverview {
	buf,err := a.StoreReadMessage(id,true,false,false)
	defer buf.Free()
	if err!=nil { return nil }
	po,_,_ := articlestore.UnpackMessage(buf.Bytes())
	over := newspolyglot.AcquireArticleOverview()
	err = zunmarshal(po,flattenP(over)...)
	if err!=nil { return nil }
	return over
}

/* ------------------------------------------------------------------------ */

func (a *ArticleDB) postReqs() bool {
	return (a.Policy!=nil) && (a.StorageW!=nil)
}
func (a *ArticleDB) ArticlePostingPost(headp *posting.HeadInfo, body []byte, ngs [][]byte, numbs []int64) (rejected bool, failed bool, err error) {
	if !a.postReqs() { return false,true,nil }
	
	bl := int64(len(headp.RAW)+2+len(body))
	ll := posting.CountLines(body)
	overv,_ := msgpack.Marshal(flatten2V(headp,bl,ll)...)
	
	decision := policies.Def(a.Policy).Decide(ngs,ll,bl)
	
	overv  = decision.CompressXover .Def()(policies.DEFLATE{},overv)
	head  := decision.CompressHeader.Def()(policies.DEFLATE{},headp.RAW)
	body   = decision.CompressBody  .Def()(policies.DEFLATE{},body)
	
	bx,e := articlestore.PackMessage(overv,head,body)
	defer bx.Free()
	if e!=nil { return false,true,e }
	
	exp := uint64(decision.ExpireAt.Unix())
	
	e = a.StoreWriteMessage(headp.MessageId,bx.Bytes(),exp)
	if e!=nil { return false,true,e }
	
	e = a.AssignArticleToGroups(ngs,numbs,exp,headp.MessageId)
	
	if e!=nil { return false,true,e }
	
	return false,false,nil
}
func (a *ArticleDB) ArticlePostingCheckPost() (possible bool) {
	if !a.postReqs() { return false }
	return true
}
func (a *ArticleDB) ArticlePostingCheckPostId(id []byte) (wanted bool, possible bool) {
	if !a.postReqs() { return true,false }
	buf,err := a.StoreReadMessage(id,true,false,false)
	if err!=nil { return true,true }
	buf.Free()
	return false,true
}

/* ------------------------------------------------------------------------ */

var _ newspolyglot.ArticleGroupDB      = (*ArticleDB)(nil)
var _ newspolyglot.ArticleDirectDB     = (*ArticleDB)(nil)
var _ newspolyglot.ArticlePostingDB    = (*ArticleDB)(nil)

