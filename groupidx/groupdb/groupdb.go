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


package groupdb

import "github.com/maxymania/storage-engines/anytree"
import "github.com/maxymania/storage-engines/anytree/gistops"
import "context"
import "errors"

var ENoSuchGroup = errors.New("ENoSuchGroup")

var groupTree   = anytree.GiST{Ops: gistops.StrkeyOps(),TreeM:24}
var articleTree = anytree.GiST{Ops:     ArticleKeyOps(),TreeM:85}

type groupEntry struct{
	root  int64
	count int64
	low   int64
	high  int64
}

type DB struct{
	inner *anytree.DB
}

type Tx struct{
	parent *DB
	inner *anytree.Tx
}
func (tx *Tx) AssignArticle(ctx context.Context,group []byte, num int64, id []byte, expire int64) error {
	var gep int64
	var ge groupEntry
	
	root,err := tx.inner.GetRoot()
	if err!=nil { return err }
	ch := make(chan anytree.Pair,1)
	tree := groupTree.WithTransaction(tx.inner)
	tree.Root = root
	cc,cancel := context.WithCancel(ctx)
	go tree.SearchGiST(cc,gistops.StrkeyLiteral(group),ch)
	p := <- ch
	cancel()
	if p.K==nil { return ENoSuchGroup }
	err = Unmarshal(p.V.Ptr,&gep)
	p.V.Free()
	if err!=nil { return err }
	
	b,err := tx.inner.Read(gep)
	
	if err!=nil { b.Free(); return err }
	
	err = Unmarshal(b.Ptr,&ge)
	b.Free()
	if err!=nil { return err }
	
	artree := articleTree.WithTransaction(tx.inner)
	artree.Root = ge.root
	
	if ge.low  > num || ge.low==0 { ge.low = num }
	if ge.high < num { ge.high = num }
	
	ge.count++
	
	err = artree.InsertGiST(ctx,ArticleKeyLiteral(num).WithUnixTime(expire),id)
	
	if err!=nil { return err }
	
	ge.root = artree.Root
	
	gedata,err := Marshal(ge)
	if err!=nil { return err }
	
	err = tx.inner.Update(gep,gedata)
	
	return err
}

