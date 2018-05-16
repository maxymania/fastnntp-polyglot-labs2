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
import "github.com/vmihailenco/msgpack"
import "sort"
import "fmt"

type Range struct{
	Beg, End int64
}
func (a *Range) Match(b *Range) bool {
	if a.End < b.Beg { return false }
	if b.End < a.Beg { return false }
	return true
}
func (a *Range) Contains(i int64) bool {
	if i < a.Beg { return false }
	if a.Beg < i { return false }
	return true
}
func (a *Range) UnionInPlace(b *Range) {
	if a.Beg>b.Beg { a.Beg = b.Beg }
	if a.End<b.End { a.End = b.End }
}
func (a *Range) String() string {
	if a==nil { return "*" }
	if a.Beg!=a.End { return fmt.Sprintf("%d-%d",a.Beg,a.End) }
	return fmt.Sprintf("%d",a.Beg)
}

type ArticleKey struct{
	Article, UnixTime *Range
}

func ArticleKeyLiteral(article int64) *ArticleKey {
	return &ArticleKey{Article:&Range{article,article}}
}

func (a *ArticleKey) WithUnixTime(ut int64) *ArticleKey {
	a.UnixTime = &Range{ut,ut}
	return a
}
func (a *ArticleKey) lowAN() int64 {
	if a.Article==nil { return 0 }
	return a.Article.Beg
}

func (a *ArticleKey) Clone() (b *ArticleKey) {
	b = new(ArticleKey)
	if a.Article!=nil {
		b.Article = new(Range)
		*(b.Article) = *(a.Article)
	}
	if a.UnixTime!=nil {
		b.UnixTime = new(Range)
		*(b.UnixTime) = *(a.UnixTime)
	}
	return
}

func (a *ArticleKey) ContainsArticle(an int64) bool {
	if a.Article==nil { return true }
	return a.Article.Contains(an)
}

func (a *ArticleKey) Match(b *ArticleKey) bool {
	if a.Article!=nil && b.Article!=nil {
		if !a.Article.Match(b.Article) { return false }
	}
	if a.UnixTime!=nil && b.UnixTime!=nil {
		if !a.UnixTime.Match(b.UnixTime) { return false }
	}
	return true
}

func (a *ArticleKey) UnionInPlace(b *ArticleKey) {
	if b.Article == nil {
		a.Article = nil
	} else if a.Article!=nil {
		a.Article.UnionInPlace(b.Article)
	}
	if b.UnixTime == nil {
		a.UnixTime = nil
	} else if a.UnixTime!=nil {
		a.UnixTime.UnionInPlace(b.UnixTime)
	}
}

func (a *ArticleKey) EncodeMsgpack(dst *msgpack.Encoder) error {
	u := uint64(0)
	if a.Article!=nil { u |= 1 }
	if a.UnixTime!=nil { u |= 2 }
	err := dst.EncodeUint(u)
	if err!=nil { return err }
	if r := a.Article; r!=nil {
		err = dst.Encode(r.Beg,r.End)
		if err!=nil { return err }
	}
	if r := a.UnixTime; r!=nil {
		err = dst.Encode(r.Beg,r.End)
		if err!=nil { return err }
	}
	return nil
}

func (a *ArticleKey) DecodeMsgpack(src *msgpack.Decoder) error {
	u,err := src.DecodeUint()
	if err!=nil { return err }
	if (u&1)!=0 {
		r := new(Range)
		err = src.Decode(&(r.Beg),&(r.End))
		if err!=nil { return err }
		a.Article = r
	}
	if (u&2)!=0 {
		r := new(Range)
		err = src.Decode(&(r.Beg),&(r.End))
		if err!=nil { return err }
		a.UnixTime = r
	}
	return nil
}

var _ msgpack.CustomEncoder = (*ArticleKey)(nil)
var _ msgpack.CustomDecoder = (*ArticleKey)(nil)



type tAK struct{}

func (t tAK) KT() interface{} {
	return new(ArticleKey)
}

func (t tAK) Consistent(E, q interface{}) bool {
	a := E.(*ArticleKey)
	switch b := q.(type) {
	case *ArticleKey:
		return a.Match(b)
	case int64:
		return a.ContainsArticle(b)
	}
	return true
}

// Union(P)=>R so that (r → p for all p in P)
func (t tAK) Union(P []anytree.Element) interface{} {
	nr := P[0].P.(*ArticleKey).Clone()
	for _,e := range P[1:] {
		x := e.P.(*ArticleKey)
		nr.UnionInPlace(x)
	}
	return nr
}

// Penalty(E¹,E²): Calculates the penalty of merging E¹ and E² under E¹.
func (t tAK) Penalty(E1,E2 interface{}) float64 {
	return 0
}

// PickSplit(P)->P¹,P²
func (t tAK) PickSplit(P []anytree.Element) (P1,P2 []anytree.Element) {
	sort.Slice(P,anytree.SortOrder(t.Compare,P))
	return P[:len(P)/2],P[len(P)/2:]
}

// Compare (E¹,E²):
// Returns <0 if E¹ preceeds E²
//         >0 if E² preceeds E¹
//          0 otherwise.
//
// This function is optional. If this function is given, IsOrdered is true automatically.
func (t tAK) Compare(E1,E2 interface{}) int {
	a1 := E1.(*ArticleKey).lowAN()
	a2 := E2.(*ArticleKey).lowAN()
	if a1<a2 { return -1 }
	if a1>a2 { return  1 }
	return 0
}


func ArticleKeyOps() *anytree.GistOps {
	var t tAK
	return &anytree.GistOps{
		KT:t.KT,
		Consistent:t.Consistent,
		Union:t.Union,
		Penalty:t.Penalty,
		PickSplit:t.PickSplit,
		Compare:t.Compare,
	}
}

