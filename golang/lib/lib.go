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
const TCP_BUFFER_SIZE = 4096 // bytes
const UDP_TIMEOUT = 1 * time.Second
const UDP_BUFFERED = 4 // messages

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

var ErrEof = errors.New("unexpected end of stream")

type TcpClient struct {
	conn net.Conn
	buf  []byte
	Log  Logger
}

func NewTcpClient(conn net.Conn) *TcpClient {
	return &TcpClient{
		conn,
		make([]byte, 0, TCP_BUFFER_SIZE),
		Logger{conn.RemoteAddr().String()},
	}
}

func (c *TcpClient) Close() {
	c.conn.Close()
}

func (c *TcpClient) Receive() bool {
	c.buf = slices.Grow(c.buf, 512)
	n, err := c.conn.Read(c.buf[len(c.buf):cap(c.buf)])
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
			return false
		}
		panic(err)
	}
	c.buf = c.buf[:len(c.buf)+n]
	return true
}

func (c *TcpClient) HasData() bool {
	if len(c.buf) > 0 {
		return true
	}
	return c.Receive()
}
func (c *TcpClient) ReadUntil(pred func([]byte) int) ([]byte, error) {
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
func (c *TcpClient) ReadAny() ([]byte, error) {
	return c.ReadUntil(func(buf []byte) int {
		return len(buf)
	})
}
func (c *TcpClient) ReadExactly(n int) ([]byte, error) {
	return c.ReadUntil(func(buf []byte) int {
		if len(buf) >= n {
			return n
		} else {
			return 0
		}
	})
}
func (c *TcpClient) ReadLine() (string, error) {
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

func (c *TcpClient) WriteAll(msg []byte) {
	for len(msg) > 0 {
		n, err := c.conn.Write(msg)
		if err != nil {
			panic(err)
		}
		msg = msg[n:]
	}
}
func (c *TcpClient) WriteLine(msg string) {
	c.WriteAll(append([]byte(msg), '\n'))
}

func ConnectTcp(addr string) (*TcpClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewTcpClient(conn), nil
}

func ServeTcp(cb func(*TcpClient) error) {
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
			client := NewTcpClient(conn)
			client.Log.Info("Connected")
			err := cb(client)
			if err != nil {
				client.Log.Error("Error: %v", err)
			} else {
				client.Log.Info("Done")
			}
			client.Close()
		}(conn)
	}
}

type UdpClient struct {
	conn         net.PacketConn
	addr         net.Addr
	msgs         chan []byte
	lastActivity time.Time
	Log          Logger
}

func (c *UdpClient) ReadMsg() []byte {
	msg, more := <-c.msgs
	if !more {
		return nil
	}
	return msg
}

func (c *UdpClient) SendMsg(msg []byte) error {
	if len(msg) > 65_507 {
		return errors.New("UDP message too large")
	}
	n, err := c.conn.WriteTo(msg, c.addr)
	if err != nil {
		return err
	}
	if n != len(msg) {
		return errors.New("UDP message only partially sent")
	}
	return nil
}

func ServeUdp(cb func(*UdpClient) error) {
	log := Logger{""}
	conn, err := net.ListenPacket("udp", ":"+strconv.Itoa(PORT))
	log.CheckFatal(err)
	defer conn.Close()
	log.Info("Server running on port %d", PORT)

	buf := make([]byte, 65536)
	clients := make(map[string]*UdpClient)
	for {
		dl_addr := ""
		dl_time := time.Time{}
		for addr, c := range clients {
			t := c.lastActivity.Add(UDP_TIMEOUT)
			if dl_addr == "" || t.Compare(dl_time) == -1 {
				dl_addr = addr
				dl_time = t
			}
		}
		conn.SetReadDeadline(dl_time)

		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				close(clients[dl_addr].msgs)
				delete(clients, dl_addr)
				continue
			}
			log.CheckFatal(err)
		}

		cid := addr.String()
		c, has := clients[cid]
		if !has {
			c = &UdpClient{conn, addr, make(chan []byte, UDP_BUFFERED), time.Now(), Logger{cid}}
			clients[cid] = c
			go func(client *UdpClient) {
				client.Log.Info("Connected")
				err := cb(client)
				if err != nil {
					client.Log.Error("Error: %v", err)
				} else {
					client.Log.Info("Done (timed out)")
				}
			}(c)
		}

		msg := slices.Clone(buf[:n])
		select {
		case c.msgs <- msg:
		default:
			c.Log.Error("Message dropped; channel buffer full")
		}
		c.lastActivity = time.Now()
	}
}
