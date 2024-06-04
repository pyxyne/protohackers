package lib

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

const PORT = 50_000

const RESET = "\x1b[0m"
const FAINT_WHITE = "\x1b[37;2m"
const BRIGHT_CYAN = "\x1b[96;22m"
const BRIGHT_YELLOW = "\x1b[93;22m"
const BRIGHT_RED = "\x1b[91;22m"

type Logger struct {
	Name string
}

func (l *Logger) log(color string, s string, args ...any) {
	now := time.Now()
	hour, min, sec := now.Clock()
	ms := now.Nanosecond() / 1_000_000
	var b strings.Builder
	b.Grow(128)
	fmt.Fprintf(&b, "%s%02d:%02d:%02d.%03d ", FAINT_WHITE, hour, min, sec, ms)
	if l.Name != "" {
		fmt.Fprintf(&b, "[%s] ", l.Name)
	}
	b.WriteString(color)
	fmt.Fprintf(&b, s, args...)
	b.WriteString(RESET)
	fmt.Println(b.String())
}
func (l *Logger) Debug(s string, args ...any) {
	l.log(RESET, s, args...)
}
func (l *Logger) Info(s string, args ...any) {
	l.log(BRIGHT_CYAN, s, args...)
}
func (l *Logger) Warn(s string, args ...any) {
	l.log(BRIGHT_YELLOW, s, args...)
}
func (l *Logger) Error(s string, args ...any) {
	l.log(BRIGHT_RED, s, args...)
}
func (l *Logger) CheckFatal(err error) {
	if err != nil {
		l.Error(err.Error())
		os.Exit(1)
	}
}
func CheckPanic(err error) {
	if err != nil {
		panic(err)
	}
}

type Client struct {
	conn net.Conn
	buf  []byte
	Log  Logger
}

var ErrEof = errors.New("unexpected end of stream")

func (c *Client) Receive() bool {
	c.buf = slices.Grow(c.buf, 512)
	n, err := c.conn.Read(c.buf[len(c.buf):cap(c.buf)])
	if err != nil {
		if err == io.EOF {
			return false
		}
		panic(err)
	}
	c.buf = c.buf[:len(c.buf)+n]
	return true
}

func (c *Client) HasData() bool {
	if len(c.buf) > 0 {
		return true
	}
	return c.Receive()
}
func (c *Client) ReadUntil(pred func([]byte) int) ([]byte, error) {
	var n int
	for {
		n = pred(c.buf)
		if n > 0 {
			break
		}
		if !c.Receive() {
			return nil, ErrEof
		}
	}
	res := slices.Clone(c.buf[:n])
	c.buf = slices.Delete(c.buf, 0, n)
	return res, nil
}
func (c *Client) ReadAny() ([]byte, error) {
	return c.ReadUntil(func(buf []byte) int {
		return len(buf)
	})
}
func (c *Client) ReadExactly(n int) ([]byte, error) {
	return c.ReadUntil(func(buf []byte) int {
		if len(buf) >= n {
			return n
		} else {
			return 0
		}
	})
}
func (c *Client) ReadLine() (string, error) {
	msg, err := c.ReadUntil(func(buf []byte) int {
		i := slices.Index(c.buf, '\n')
		if i != -1 {
			return i + 1
		} else {
			return 0
		}
	})
	if err != nil {
		return "", err
	} else {
		return string(msg[:len(msg)-1]), err
	}
}

func (c *Client) WriteAll(msg []byte) {
	for len(msg) > 0 {
		n, err := c.conn.Write(msg)
		if err != nil {
			panic(err)
		}
		msg = msg[n:]
	}
}
func (c *Client) WriteLine(msg string) {
	c.WriteAll(append([]byte(msg), '\n'))
}

func ServeTcp(cb func(*Client) error) {
	log := Logger{""}
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(PORT))
	log.CheckFatal(err)
	defer listener.Close()
	log.Info("Server running on port %d", PORT)
	for {
		conn, err := listener.Accept()
		log.CheckFatal(err)
		go func(conn net.Conn) {
			defer conn.Close()
			client := &Client{conn, make([]byte, 0, 4096), Logger{conn.RemoteAddr().String()}}
			client.Log.Info("Connected")
			err := cb(client)
			if err != nil {
				client.Log.Error("Error: %v", err)
			} else {
				client.Log.Info("Done")
			}
		}(conn)
	}
}
