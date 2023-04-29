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

	recvErr   error
	recvClose chan interface{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func newStream(id streamID, send sender) *stream {
	return &stream{
		id:        id,
		sender:    send,
		recv:      make(chan *streamMessage, 1),
		recvClose: make(chan interface{}),
	}
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
			go func() {
				for range s.recv {
				}
			}()
			s.wg.Wait()
			close(s.recv)
		}
	})
	return nil
}

func (s *stream) send(mt messageType, flags uint8, b []byte) error {
	return s.sender.send(uint32(s.id), mt, flags, b)
}

func (s *stream) receive(ctx context.Context, msg *streamMessage) error {
	s.wg.Add(1)
	defer s.wg.Done()

	select {
	case <-s.recvClose:
		return s.recvErr
	default:
	}

	select {
	case <-s.recvClose:
		return s.recvErr
	case s.recv <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type sender interface {
	send(uint32, messageType, uint8, []byte) error
}
