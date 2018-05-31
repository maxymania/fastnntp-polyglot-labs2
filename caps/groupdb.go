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

import "github.com/maxymania/fastnntp-polyglot"
import "github.com/maxymania/fastnntp-polyglot/postauth"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupover"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx"

type GroupDB struct{
	groupidx.GroupIndex
	groupover.GroupOverview
}
func (g *GroupDB) GroupHeadFilterWithAuth(rank postauth.AuthRank, ngs [][]byte) ([][]byte, error) {
	i := 0
	var buf []byte
	for _,group := range ngs {
		sad := g.GroupOverviewGet(group,buf)
		if len(sad)==0 { continue }
		buf = sad
		if rank.TestStatus(sad[0]) { continue }
		ngs[i] = group
		i++
	}
	
	return ngs[:i],nil
}
func (g *GroupDB) GroupRealtimeList(targ func(group []byte, high, low int64, status byte)) bool {
	return g.GroupOverviewList(func(group []byte, statusAndDescr []byte){
		_,low,high,ok := g.GroupRealtimeQuery(group)
		if !ok { return }
		targ(group,high,low,statusAndDescr[0])
	})
}
func (g *GroupDB) GroupStaticList(targ func(group []byte, descr []byte)) bool {
	return g.GroupOverviewList(func(group []byte, statusAndDescr []byte){
		targ(group,statusAndDescr[1:])
	})
}

var _ postauth.GroupHeadCacheWithAuth = (*GroupDB)(nil)
var _ newspolyglot.GroupHeadDB     = (*GroupDB)(nil)
var _ newspolyglot.GroupRealtimeDB = (*GroupDB)(nil)
var _ newspolyglot.GroupStaticDB   = (*GroupDB)(nil)

