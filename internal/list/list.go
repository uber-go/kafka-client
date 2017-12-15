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
	"errors"
	"math"
)

type (
	// listNode is a single linked list node
	listNode struct {
		value int64
		prev  int
		next  int
	}

	// Address refers to the internal address for a list item
	Address int

	// IntegerList refers to a linked list of integers
	// with fixed maximum capacity
	IntegerList struct {
		array        []listNode
		size         int
		capacity     int
		head         int
		freeListHead int
	}
)

const (
	// Null refers to the null list address
	Null = Address(-1)
	// beginAddr is the index where the actual data starts
	// the first and second entries are *special* - they serve
	// as the head of linked list and head of free list respectively
	// Having the head as part of the list helps avoid branches (ifs)
	// during add/remove operations
	beginAddr = 2
)

var (
	// ErrEmpty indicates that the list is empty
	ErrEmpty = errors.New("list is empty")
	// ErrCapacity indicates that the list is out of capacity
	ErrCapacity = errors.New("list out of capacity")
	// ErrAddrOutOfRange indicates the provided address is out of range
	ErrAddrOutOfRange = errors.New("list address out of range")
	// ErrAddrMissingValue indicates that the requested address does not contain any value
	ErrAddrMissingValue = errors.New("list address missing value")
)

// NewIntegerList returns a linked list of integers with a fixed capacity
// this list is tailor made to keep track of out of order message offsets
// while processing kafka messages. Following are the allowed operations on
// this list:
//
// Add(int64):
//  Adds the given value to the end of list. Returns an address where this
//  value is stored within the list. This address must be provided to remove
//  this item from the list later
//
// Remove(listAddr):
//  Removes the item with the given address from the list. Returns address out
//  of range error if the address is invalid. The provided address is the address
//  previously received from an Add operation
//
// PeekHead():
//  Returns the value at the head of the list without removing the item from the list
//  This operation is used to identify the same commit level during periodic checkpoints
//
// Note: This list implementation uses math.MinInt64 as a special sentinel value
//
// Implementation Notes:
// The underlying implementation uses a fixed size array where each item in the array is
// a linked list node. In addition to the array, there is also a free list which also
// refers to the same backing array i.e. the implementation uses an intrusive free list
// This implementation choice is chosen for the following reasons:
//  - No dynamic memory allocation needed
//  - Array based list is very cache friendly
//  - With this list, keeping track of out of order offsets is O(1) overhead per message
func NewIntegerList(capacity int) *IntegerList {
	list := &IntegerList{
		array:    make([]listNode, capacity+beginAddr),
		capacity: capacity,
	}
	list.init()
	return list
}

// Add adds the value to the end of list. Returns the address
// where this value is stored on success
func (list *IntegerList) Add(value int64) (Address, error) {
	if list.empty(list.freeListHead) {
		return Null, ErrCapacity
	}
	// remove first node from free list and add it to list
	nextAddr := list.array[list.freeListHead].next
	list.remove(nextAddr)
	list.array[nextAddr].value = value
	list.append(list.head, nextAddr)
	list.size++

	return Address(nextAddr), nil
}

// Get returns the value at the given address
func (list *IntegerList) Get(getAddr Address) (int64, error) {
	addr := int(getAddr)
	if !list.validAddr(addr) {
		return 0, ErrAddrOutOfRange
	}
	if list.Empty() {
		return 0, ErrEmpty
	}
	value := list.array[addr].value
	if value == math.MinInt64 {
		return value, ErrAddrMissingValue
	}
	return value, nil
}

// Remove removes the item stored at the specified address
// from the list
func (list *IntegerList) Remove(removeAddr Address) error {
	addr := int(removeAddr)
	if !list.validAddr(addr) {
		return ErrAddrOutOfRange
	}
	list.remove(addr)
	list.append(list.freeListHead, addr)
	list.size--
	return nil
}

// PeekHead returns the value at the head of the list if
// the list is not empty. If the list empty, error is returned
func (list *IntegerList) PeekHead() (int64, error) {
	if list.Empty() {
		return 0, ErrEmpty
	}
	firstAddr := list.array[list.head].next
	return list.array[firstAddr].value, nil
}

// Size returns the size of the list
func (list *IntegerList) Size() int {
	return list.size
}

// Empty returns true if the list is empty
func (list *IntegerList) Empty() bool {
	return list.size == 0
}

func (list *IntegerList) empty(head int) bool {
	return list.array[head].next == head
}

func (list *IntegerList) append(head int, newAddr int) {
	list.array[newAddr].next = head
	list.array[newAddr].prev = list.array[head].prev
	list.array[list.array[head].prev].next = newAddr
	list.array[head].prev = newAddr
}

func (list *IntegerList) remove(addr int) {
	list.array[addr].value = math.MinInt64
	list.array[list.array[addr].prev].next = list.array[addr].next
	list.array[list.array[addr].next].prev = list.array[addr].prev
}

// asArray returns the list as array. Only for unit tests
func (list *IntegerList) asArray() []int64 {
	var i int
	result := make([]int64, list.size)
	next := list.array[list.head].next
	for next != list.head {
		result[i] = list.array[next].value
		next, i = list.array[next].next, i+1
	}
	return result
}

func (list *IntegerList) validAddr(addr int) bool {
	return addr >= beginAddr && addr < len(list.array)
}

func (list *IntegerList) init() {
	list.head = 0
	list.freeListHead = 1
	list.array[list.head].next = list.head
	list.array[list.head].prev = list.head
	list.array[list.freeListHead].next = beginAddr
	list.array[list.freeListHead].prev = len(list.array) - 1

	for i := beginAddr; i < len(list.array); i++ {
		list.array[i].prev = i - 1
		list.array[i].next = i + 1
		list.array[i].value = math.MinInt64
	}
	list.array[len(list.array)-1].value = math.MinInt64
	list.array[len(list.array)-1].next = list.freeListHead
}
