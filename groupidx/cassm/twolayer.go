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


package cassm

import "github.com/gocql/gocql"
import "time"
import "github.com/maxymania/fastnntp-polyglot-labs2/groupidx"

/*
A two-level Datastore that, instead of storing an entire Newsgroup in a partition
divides it up into blocks of up to approx. 16 million entries. A seperate hash
partition is created (in a seperate table) to keept track of those partitions,
especially to allow ordered traversion of all partitions in the korrect order.
*/
type N2LayerGroupDB struct{
	Granularity
	unimplemented
	Session     *gocql.Session
	OnAssign    gocql.Consistency
	OnIncrement gocql.Consistency
}

/*
Validates the Object arguments such as OnIncrement.

Should be called before the object is being used, for reliability.
*/
func (g *N2LayerGroupDB) Validate(){
	switch g.OnIncrement {
	case gocql.Any,gocql.Two,gocql.Three:
		g.OnIncrement = gocql.Quorum
	}
}

func n2l1(i uint64) uint64 {
	return i & ^uint64(0xFFFFFF)
}

func (g *N2LayerGroupDB) AssignArticleToGroup(group []byte, num, exp uint64, id []byte) error {
	gid,err := getUUID(g.Session,group)
	if err!=nil { return err }
	
	ept,coarse := g.convert(exp)
	secs := int64(time.Until(time.Unix(int64(ept),0))/time.Second) + 1
	
	nxs := n2l1(num)
	
	err = qExec(g.Session.Query(`
		INSERT INTO agstat2l2 (identifier,articlepart,articlenum,messageid) VALUES (?,?,?,?) USING TTL ?
	`,gid,nxs,num,id,secs).Consistency(g.OnAssign))
	if err!=nil { return err }
	err = qExec(g.Session.Query(`
		INSERT INTO agstat1l2 (identifier,articlepart,expiresat) VALUES (?,?,?) IF NOT EXISTS USING TTL ?
	`,gid,nxs,exp,secs).Consistency(g.OnAssign))
	if err!=nil { return err }
	err = qExec(g.Session.Query(`
		UPDATE agstat1l2 USING TTL ? SET expiresat = ? WHERE identifier = ? AND articlepart = ? IF expiresat < ?
	`,secs,exp,gid,nxs,exp).Consistency(g.OnAssign))
	if err!=nil { return err }
	
	err = qExec(g.Session.Query(`
		UPDATE agrpcnt SET number = number + 1 WHERE identifier = ? AND livesuntil = ?
	`,gid,coarse).Consistency(g.OnIncrement))
	
	return err
}
func (g *N2LayerGroupDB) AssignArticleToGroups(groups [][]byte, nums []int64, exp uint64, id []byte) (err error) {
	gids := make([]gocql.UUID,len(groups))
	for i,group := range groups {
		gids[i],err = getUUID(g.Session,group)
		if err!=nil { return }
	}
	ept,coarse := g.convert(exp)
	secs := int64(time.Until(time.Unix(int64(ept),0))/time.Second) + 1
	
	batch := g.Session.NewBatch(gocql.UnloggedBatch)
	batch.SetConsistency(g.OnAssign)
	insbt := g.Session.NewBatch(gocql.UnloggedBatch)
	insbt.SetConsistency(g.OnAssign)
	updtb := g.Session.NewBatch(gocql.UnloggedBatch)
	updtb.SetConsistency(g.OnAssign)
	
	ctrbt := g.Session.NewBatch(gocql.CounterBatch)
	ctrbt.SetConsistency(g.OnIncrement)
	for i,gid := range gids {
		nxs := n2l1(uint64(nums[i]))
		batch.Query(`
			INSERT INTO agstat2l2 (identifier,articlepart,articlenum,messageid) VALUES (?,?,?,?) USING TTL ?
		`,gid,nxs,nums[i],id,secs)
		insbt.Query(`
			INSERT INTO agstat1l2 (identifier,articlepart,expiresat) VALUES (?,?,?) IF NOT EXISTS USING TTL ?
		`,gid,nxs,exp,secs)
		updtb.Query(`
			UPDATE agstat1l2 USING TTL ? SET expiresat = ? WHERE identifier = ? AND articlepart = ? IF expiresat < ?
		`,secs,exp,gid,nxs,exp)
		ctrbt.Query(`
			UPDATE agrpcnt SET number = number + 1 WHERE identifier = ? AND livesuntil = ?
		`,gid,coarse)
	}
	err = g.Session.ExecuteBatch(batch)
	if err!=nil { return }
	err = g.Session.ExecuteBatch(insbt)
	if err!=nil { return }
	err = g.Session.ExecuteBatch(updtb)
	if err!=nil { return }
	err = g.Session.ExecuteBatch(ctrbt)
	
	return
}

func (g *N2LayerGroupDB) GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool) {
	u,err := peekUUID(g.Session,group)
	if err!=nil { return }
	now := time.Now().UTC().Unix()
	
	var plow,phigh int64
	
	pok := qIter(g.Session.Query(`
		SELECT MIN(articlepart),MAX(articlepart)
		FROM agstat1l2
		WHERE identifier = ?
	`,u)).scanclose(&plow,&phigh)
	if !pok { ok = true ; return }
	
	pok = qIter(g.Session.Query(`
		SELECT MIN(articlenum)
		FROM agstat2l2
		WHERE identifier = ? and articlepart = ?
	`,u,plow)).scanclose(&low)
	
	ok = true
	if !pok { return }
	
	pok = qIter(g.Session.Query(`
		SELECT MAX(articlenum)
		FROM agstat2l2
		WHERE identifier = ? and articlepart = ?
	`,u,phigh)).scanclose(&high)
	
	if !pok { high,number = low,1 ; return }
	
	pok = qIter(g.Session.Query(`
		SELECT SUM(number) FROM agrpcnt WHERE identifier = ? AND livesuntil >= ?
	`,u,now)).scanclose(&number)
	if !pok {
		number = 1+high-low
	} else {
		num := 1+high-low
		if number > num { number = num }
	}
	
	return
}

// Efficient traversal of a newsgroup.
func (g *N2LayerGroupDB) ArticleGroupList(group []byte, first, last int64, targ func(int64)) {
	u,err := peekUUID(g.Session,group)
	if err!=nil { return }
	iter1 := qIter(g.Session.Query(`
		SELECT articlepart
		FROM agstat1l2
		WHERE identifier = ? AND articlepart >= ? AND articlepart <= ?
	`,u,n2l1(uint64(first)),n2l1(uint64(last))).PageSize(1<<16).Prefetch(.25))
	defer iter1.Close()
	var part int64
	var iter iter
	defer iter.sClose()
	for iter1.Scan(&part) {
		iter.place(qIter(g.Session.Query(`
			SELECT articlenum
			FROM agstat2l2
			WHERE identifier = ?
			AND articlepart = ?
			AND articlenum >= ?
			AND articlenum <= ?
		`,u,part,first,last).PageSize(1<<16).Prefetch(.25)))
		var num int64
		for iter.Scan(&num) {
			targ(num)
		}
	}
}

func (g *N2LayerGroupDB) ListArticleGroupRaw(group []byte, first, last int64, targ func(int64, []byte)) {
	u,err := peekUUID(g.Session,group)
	if err!=nil { return }
	iter1 := qIter(g.Session.Query(`
		SELECT articlepart
		FROM agstat1l2
		WHERE identifier = ? AND articlepart >= ? AND articlepart <= ?
	`,u,n2l1(uint64(first)),n2l1(uint64(last))).PageSize(1<<16).Prefetch(.25))
	defer iter1.Close()
	var part int64
	var iter iter
	defer iter.sClose()
	var id []byte
	for iter1.Scan(&part) {
		iter.place(qIter(g.Session.Query(`
			SELECT articlenum, messageid
			FROM agstat2l2
			WHERE identifier = ?
			AND articlepart = ?
			AND articlenum >= ?
			AND articlenum <= ?
		`,u,part,first,last).PageSize(1<<16).Prefetch(.25)))
		var num int64
		for iter.Scan(&num,&id) {
			targ(num,id)
		}
	}
}

func (g *N2LayerGroupDB) ArticleGroupStat(group []byte, num int64, id_buf []byte) ([]byte, bool) {
	u,err := peekUUID(g.Session,group)
	if err!=nil { return nil,false }
	nxs := n2l1(uint64(num))
	
	id := id_buf
	
	ok := qIter(g.Session.Query(`
		SELECT messageid FROM agstat2l2 WHERE identifier = ? AND articlepart = ? AND articlenum = ?
	`,u,nxs,num)).scanclose(&id)
	
	return id,ok
}
func (g *N2LayerGroupDB) ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	u,err := peekUUID(g.Session,group)
	if err!=nil { return }
	sym := ">"
	dir := "ASC"
	if backward { sym = "<"; dir = "DESC" }
	nxs := n2l1(uint64(i))
	var nxs2 uint64
	done := qIter(g.Session.Query(`
		SELECT articlepart FROM agstat1l2 WHERE identifier = ? AND articlepart `+sym+` ? ORDER BY articlepart `+dir+` LIMIT 1
	`,u,nxs)).scanclose(&nxs2)
	
	ok = qIter(g.Session.Query(`
		SELECT articlenum,messageid FROM agstat2l2 WHERE identifier = ? AND articlepart = ? AND articlenum `+sym+` ?
			ORDER BY articlenum `+dir+` LIMIT 1
	`,u,nxs,i)).scanclose(&ni,&id)
	
	if ok { return }
	if !done { return }
	
	ok = qIter(g.Session.Query(`
		SELECT articlenum,messageid FROM agstat2l2 WHERE identifier = ? AND articlepart = ? AND articlenum `+sym+` ?
			ORDER BY articlenum `+dir+` LIMIT 1
	`,u,nxs2,i)).scanclose(&ni,&id)
	
	return
}

var _ groupidx.GroupIndex = (*N2LayerGroupDB)(nil)

