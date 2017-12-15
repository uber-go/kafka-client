// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package list

import (
	"testing"

	"math/rand"
	"time"

	"github.com/stretchr/testify/suite"
)

type (
	ListTestSuite struct {
		suite.Suite
	}

	testItem struct {
		addr  Address
		value int64
	}
)

func TestListTestSuite(t *testing.T) {
	suite.Run(t, new(ListTestSuite))
}

func (s *ListTestSuite) SetupTest() {
	rand.Seed(time.Now().UnixNano())
}

func (s *ListTestSuite) TestCapacityOfOne() {
	list := NewIntegerList(1)
	for i := 0; i < 10; i++ {
		s.Equal(0, list.size)
		s.True(list.Empty())
		addr := s.testAddNoErr(list, 5)
		s.testPeekNoErr(list, 5)
		s.testAddErr(list, 6)
		s.testRemove(list, addr)
		s.testPeekErr(list)
	}
}

func (s *ListTestSuite) TestCapacityOfN() {
	cap := 128
	list := NewIntegerList(cap)
	for i := 0; i < 1; i++ {
		items := make([]testItem, cap)
		removed := make(map[int64]struct{})
		for c := 0; c < cap; c++ {
			item := testItem{value: int64(c)}
			item.addr = s.testAddNoErr(list, c)
			items[c] = item
		}
		s.testAddErr(list, cap)
		// remove items from list in random order
		s.shuffle(items)
		for c := 0; c < cap/2; c++ {
			s.testRemove(list, items[c].addr)
			removed[items[c].value] = struct{}{}
			s.testSanity(list, removed)
		}
		for c := 0; c < cap/2; c++ {
			item := testItem{value: int64(cap + c)}
			item.addr = s.testAddNoErr(list, cap+c)
			items[c] = item
		}
		s.testAddErr(list, cap)
		s.shuffle(items)
		for c := 0; c < cap; c++ {
			s.testRemove(list, items[c].addr)
			removed[items[c].value] = struct{}{}
			s.testSanity(list, removed)
		}
		s.True(list.Empty())
		s.Equal(0, list.Size())
		_, err := list.Get(beginAddr)
		s.Equal(ErrEmpty, err)
	}
}

func (s *ListTestSuite) TestAddrOutOfRange() {
	list := NewIntegerList(1)
	for i := 3; i < 10; i++ {
		err := list.Remove(Address(i))
		s.Equal(ErrAddrOutOfRange, err)
		_, err = list.Get(Address(i))
		s.Equal(ErrAddrOutOfRange, err)
	}
}

func (s *ListTestSuite) TestAddrMissingValue() {
	list := NewIntegerList(2)
	addr, err := list.Add(888)
	s.NoError(err)
	_, err = list.Get(Address(addr + 1))
	s.Equal(ErrAddrMissingValue, err)
}

func (s *ListTestSuite) testAddNoErr(list *IntegerList, value int) Address {
	size := list.Size()
	addr, err := list.Add(int64(value))
	s.NoError(err)
	s.False(list.Empty())
	s.Equal(size+1, list.Size())
	return addr
}

func (s *ListTestSuite) testAddErr(list *IntegerList, value int) {
	addr, err := list.Add(int64(value))
	s.Equal(ErrCapacity, err)
	s.Equal(Null, addr)
}

func (s *ListTestSuite) testRemove(list *IntegerList, addr Address) {
	size := list.Size()
	err := list.Remove(addr)
	s.NoError(err)
	s.Equal(size-1, list.Size())
	if size == 0 {
		s.True(list.Empty())
	}
}

func (s *ListTestSuite) testPeekNoErr(list *IntegerList, expected int) {
	val, err := list.PeekHead()
	s.NoError(err)
	s.Equal(int64(expected), val)
}

func (s *ListTestSuite) testPeekErr(list *IntegerList) {
	_, err := list.PeekHead()
	s.Equal(ErrEmpty, err)
	s.True(list.Empty())
}

// assert list does not contain removed entries
// assert items in the list are in ascending order
func (s *ListTestSuite) testSanity(list *IntegerList, removed map[int64]struct{}) {
	array := list.asArray()
	if len(array) == 0 {
		return
	}

	prev := array[0]
	_, ok := removed[prev]
	s.False(ok)
	for i := 1; i < len(array); i++ {
		_, ok := removed[array[i]]
		s.False(ok)
		s.True(array[i] > prev)
		prev = array[i]
	}
}

func (s *ListTestSuite) shuffle(items []testItem) {
	n := len(items)
	for i := n - 1; i >= 0; i-- {
		idx := rand.Intn(n)
		items[i], items[idx] = items[idx], items[i]
		n--
	}
}
