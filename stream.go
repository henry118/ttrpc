/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package ttrpc

import (
	"context"
	"sync"
)

type streamID uint32

type streamMessage struct {
	header  messageHeader
	payload []byte
}

type stream struct {
	id     streamID
	sender sender
	recv   chan *streamMessage
	mc     chan *streamMessage

	recvErr   error
	recvClose chan interface{}
	closeOnce sync.Once
}

func newStream(ctx context.Context, id streamID, send sender) *stream {
	s := &stream{
		id:        id,
		sender:    send,
		recv:      make(chan *streamMessage, 1),
		mc:        make(chan *streamMessage, 1),
		recvClose: make(chan interface{}),
	}

	go func(ctx context.Context) {
		defer close(s.recv)
		for {
			select {
			case <-s.recvClose:
				return
			default:
			}
			select {
			case <-s.recvClose:
				return
			case msg := <-s.mc:
				s.recv <- msg
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	return s
}

func (s *stream) closeWithError(err error) error {
	s.closeOnce.Do(func() {
		if s.recv != nil {
			if err != nil {
				s.recvErr = err
			} else {
				s.recvErr = ErrClosed
			}
			close(s.recvClose)
		}
	})
	return nil
}

func (s *stream) send(mt messageType, flags uint8, b []byte) error {
	return s.sender.send(uint32(s.id), mt, flags, b)
}

func (s *stream) receive(ctx context.Context, msg *streamMessage) error {
	select {
	case s.mc <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type sender interface {
	send(uint32, messageType, uint8, []byte) error
}
