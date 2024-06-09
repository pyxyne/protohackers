package main

import (
	"fmt"
	"pyxyne/protohackers/lib"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func parseU31(s []byte) (int, bool) {
	val, err := strconv.ParseUint(string(s), 10, 31)
	if err != nil {
		return 0, false
	}
	return int(val), true
}

// Application layer
func lineReverser(session *Session) {
	var buf []byte
	for msg := range session.Recv {
		buf = append(buf, msg...)
		for {
			i := slices.Index(buf, '\n')
			if i == -1 {
				break
			}
			line := slices.Clone(buf[:i])
			buf = slices.Delete(buf, 0, i+1)

			session.Log.Debug("<-- %q", line)
			slices.Reverse(line)
			session.Log.Debug("--> %q", line)

			line = append(line, '\n')
			session.Send <- line
		}
	}
	session.Log.Info("Done")
}

type EventType int

const (
	EVENT_DATA EventType = iota
	EVENT_ACK
)

type Event struct {
	ty   EventType
	pos  int
	data []byte
}

type Session struct {
	Recv   chan []byte
	Send   chan []byte
	Log    lib.Logger
	events chan Event
	closed atomic.Bool
}

func sendMsg(c *lib.UdpClient, f string, args ...any) {
	msg := fmt.Sprintf(f, args...)
	c.Log.Debug("-> %q", msg)
	c.SendMsg([]byte(msg))
}

var unescaper = strings.NewReplacer("\\\\", "\\", "\\/", "/")
var escaper = strings.NewReplacer("\\", "\\\\", "/", "\\/")

const RETRANS_TIMEOUT = 1 * time.Second
const MAX_CHUNK_SIZE = 480 // to account for header and escapes
const MAX_CHUNKS = 5

func newSession(id int, client *lib.UdpClient) *Session {
	s := &Session{
		Recv:   make(chan []byte, 20),
		Send:   make(chan []byte, 20),
		Log:    lib.Logger{Name: fmt.Sprintf("session%d", id)},
		events: make(chan Event),
		closed: atomic.Bool{},
	}
	go lineReverser(s)
	go func() {
		recved := 0
		acked := 0
		nyack := make([]byte, 0, 32)
		retransTimer := time.NewTimer(0)
		<-retransTimer.C
		expiryTimer := time.NewTimer(60 * time.Second)

		closeSession := func(loud bool) {
			s.closed.Store(true)
			close(s.Recv)
			if loud {
				sendMsg(client, "/close/%d/", id)
			}
		}

		sendData := func(pos int, data []byte) {
			off := 0
			chunks := 0
			for off < len(data) && chunks < MAX_CHUNKS {
				chunkSize := min(MAX_CHUNK_SIZE, len(data)-off)
				sendMsg(client, "/data/%d/%d/%s/", id, pos+off, escaper.Replace(string(data[off:off+chunkSize])))
				off += chunkSize
				chunks++
			}
			retransTimer.Reset(RETRANS_TIMEOUT)
		}

		retransmit := func(extra string) {
			lib.Assert(len(nyack) > 0)
			s.Log.Warn("Retransmitting %d bytes (%s)", len(nyack), extra)
			sendData(acked, nyack)
		}

		for {
			select {
			case ev, ok := <-s.events:
				if !ok {
					closeSession(false)
					return
				}
				switch ev.ty {
				case EVENT_DATA:
					if ev.pos > recved {
						s.Log.Warn("Missed %d bytes, requesting retransmit", ev.pos-recved)
					} else if ev.pos+len(ev.data) > recved {
						newData := ev.data[recved-ev.pos:]
						recved += len(newData)
						s.Recv <- newData
					}
					sendMsg(client, "/ack/%d/%d/", id, recved)
				case EVENT_ACK:
					if ev.pos <= acked {
						s.Log.Warn("Ignoring duplicate ACK")
					} else if ev.pos > acked+len(nyack) {
						s.Log.Error("Data not sent yet was acked")
						closeSession(true)
						return
					} else {
						nyack = slices.Delete(nyack, 0, ev.pos-acked)
						acked = ev.pos
						if len(nyack) > 0 {
							retransmit("prompted")
						} else {
							retransTimer.Stop()
						}
					}
				}
			case msg := <-s.Send:
				sendData(acked+len(nyack), msg)
				nyack = append(nyack, msg...)
			case <-retransTimer.C:
				retransmit("timeout")
			case <-expiryTimer.C:
				s.Log.Warn("Session timed out")
				closeSession(false)
				return
			}
		}
	}()
	return s
}

func P7() {
	lib.ServeUdp(60*time.Second, func(c *lib.UdpClient) error {
		sessions := make(map[int]*Session)

		processMsg := func(msg []byte) bool {
			if len(msg) < 2 || msg[0] != '/' || msg[len(msg)-1] != '/' {
				return false
			}

			var parts [][]byte
			prevI := -1
			for i, c := range msg {
				if c == '/' && (i == 0 || msg[i-1] != '\\') {
					if prevI != -1 {
						parts = append(parts, []byte(unescaper.Replace(string(msg[prevI+1:i]))))
					}
					prevI = i
				}
			}

			if len(parts) < 2 {
				return false
			}
			sessionId, ok := parseU31(parts[1])
			if !ok {
				return false
			}
			session, sessionOpen := sessions[sessionId]
			if sessionOpen && session.closed.Load() {
				delete(sessions, sessionId)
				session = nil
				sessionOpen = false
			}

			switch string(parts[0]) {
			case "connect":
				if len(parts) != 2 {
					return false
				}
				if !sessionOpen {
					sessions[sessionId] = newSession(sessionId, c)
				}
				sendMsg(c, "/ack/%d/0/", sessionId)
			case "data":
				if len(parts) != 4 {
					return false
				}
				pos, ok := parseU31(parts[2])
				if !ok {
					return false
				}
				data := parts[3]
				if !sessionOpen {
					c.Log.Warn("Data in closed session")
					sendMsg(c, "/close/%d/", sessionId)
					break
				}
				session.events <- Event{
					ty:   EVENT_DATA,
					pos:  pos,
					data: data,
				}
			case "ack":
				if len(parts) != 3 {
					return false
				}
				length, ok := parseU31(parts[2])
				if !ok {
					return false
				}
				if !sessionOpen {
					c.Log.Warn("Ack in closed session")
					sendMsg(c, "/close/%d/", sessionId)
					break
				}
				session.events <- Event{
					ty:  EVENT_ACK,
					pos: length,
				}
			case "close":
				if len(parts) != 2 {
					return false
				}
				if sessionOpen {
					close(session.events)
					delete(sessions, sessionId)
				}
				sendMsg(c, "/close/%d/", sessionId)
			default:
				return false
			}
			return true
		}

		for msg := range c.Msgs {
			c.Log.Debug("<- %q", msg)
			if !processMsg(msg) {
				c.Log.Warn("Invalid message")
			}
		}
		for _, session := range sessions {
			close(session.events)
		}
		return nil
	})
}
