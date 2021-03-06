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


package groupidx

type GroupIndex interface{
	// known from "github.com/maxymania/fastnntp-polyglot"
	GroupHeadInsert(groups [][]byte, buf []int64) ([]int64, error)
	GroupHeadRevert(groups [][]byte, nums []int64) error
	ArticleGroupStat(group []byte, num int64, id_buf []byte) ([]byte, bool)
	ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool)
	GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool)
	
	// Newly introduced.
	AssignArticleToGroup(group []byte, num, exp uint64, id []byte) error
	AssignArticleToGroups(groups [][]byte, nums []int64, exp uint64, id []byte) error /* Bulk-version*/
	ListArticleGroupRaw(group []byte, first, last int64, targ func(int64, []byte))
}

/* Don't use this!!! */
type P_GIPrototype GroupIndex

/*type gGroupIndexExList interface{
	// known from "github.com/maxymania/fastnntp-polyglot"
	ArticleGroupList(group []byte, first, last int64, targ func(int64))
}*/

