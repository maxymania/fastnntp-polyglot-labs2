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
import "errors"

var EUnimplemented = errors.New("EUnimplemented")
var ENoSuchGroup = errors.New("Missing Group")

func initGen(session *gocql.Session) {
	session.Query(`
	CREATE TABLE IF NOT EXISTS newsgroups (
		groupname blob PRIMARY KEY,
		identifier uuid
	)
	`).Exec()
	session.Query(`
	CREATE TABLE IF NOT EXISTS agrpcnt (
		identifier uuid,
		livesuntil bigint,
		number counter,
		PRIMARY KEY(identifier,livesuntil)
	)
	`).Exec()
}
func Initialize(session *gocql.Session) {
	initGen(session)
	session.Query(`
	CREATE TABLE IF NOT EXISTS agstat (
		identifier uuid,
		articlenum bigint,
		messageid blob,
		PRIMARY KEY(identifier,articlenum)
	)
	`).Exec()
}
func InitializeN2Layer(session *gocql.Session) {
	initGen(session)
	session.Query(`
	CREATE TABLE IF NOT EXISTS agstat1l2 (
		identifier uuid,
		articlepart bigint,
		expiresat bigint,
		PRIMARY KEY(identifier,articlepart)
	)
	`).Exec()
	session.Query(`
	CREATE TABLE IF NOT EXISTS agstat2l2 (
		identifier uuid,
		articlepart bigint,
		articlenum bigint,
		messageid blob,
		PRIMARY KEY((identifier,articlepart),articlenum)
	)
	`).Exec()
}

func getUUID(session *gocql.Session,name []byte) (u gocql.UUID,err error) {
	iter := session.Query(`
	SELECT identifier FROM newsgroups WHERE groupname = ?
	`,name).Consistency(gocql.One).Iter()
	ok := iter.Scan(&u)
	iter.Close()
	if !ok {
		u,err = gocql.RandomUUID()
		if err!=nil { return }
		session.Query(`
		INSERT INTO newsgroups (groupname,identifier) VALUES (?,?)
		IF NOT EXISTS
		`,name,u).Consistency(gocql.Any).Exec()
		iter = session.Query(`
		SELECT identifier FROM newsgroups WHERE groupname = ?
		`,name).Consistency(gocql.One).Iter()
		ok = iter.Scan(&u)
		iter.Close()
		if !ok {
			err = ENoSuchGroup
		}
	}
	return
}
func peekUUID(session *gocql.Session,name []byte) (u gocql.UUID,err error) {
	iter := session.Query(`
	SELECT identifier FROM newsgroups WHERE groupname = ?
	`,name).Consistency(gocql.One).Iter()
	ok := iter.Scan(&u)
	iter.Close()
	if !ok {
		err = ENoSuchGroup
	}
	return
}

type unimplemented struct{}

// EUnimplemented
func (unimplemented) GroupHeadInsert(groups [][]byte, buf []int64) ([]int64, error) { return nil,EUnimplemented }

// EUnimplemented
func (unimplemented) GroupHeadRevert(groups [][]byte, nums []int64) error { return EUnimplemented }

