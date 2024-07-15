package lib

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const PORT = 50_000
const TCP_BUFFER = 4096 // bytes
const UDP_BUFFER = 20   // messages

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
func Assert(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}

var ErrEof = errors.New("unexpected end of stream")

type TcpClient struct {
	conn net.Conn
	buf  []byte
	Log  Logger
}

func NewTcpClient(conn net.Conn, name string) *TcpClient {
	return &TcpClient{
		conn,
		make([]byte, 0, TCP_BUFFER),
		Logger{name},
	}
}

func (c *TcpClient) Close() {
	c.conn.Close()
}

func (c *TcpClient) Receive() bool {
	c.buf = slices.Grow(c.buf, 512)
	n, err := c.conn.Read(c.buf[len(c.buf):cap(c.buf)])
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) {
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
	c.Log.Debug("<- %q", res)
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
	c.Log.Debug("-> %q", msg)
	for len(msg) > 0 {
		n, err := c.conn.Write(msg)
		if err != nil {
			if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
				c.Log.Error("Couldn't send: socket closed")
				return
			}
			panic(err)
		}
		msg = msg[n:]
	}
}
func (c *TcpClient) WriteLine(msg string) {
	c.WriteAll(append([]byte(msg), '\n'))
}

type BinReader struct {
	c   *TcpClient
	Err error
}

func (c *TcpClient) GetBinReader() *BinReader {
	return &BinReader{c, nil}
}
func (r *BinReader) U8() uint8 {
	var bytes []byte
	if r.Err == nil {
		bytes, r.Err = r.c.ReadExactly(1)
	}
	if r.Err != nil {
		return 0
	}
	return bytes[0]
}
func (r *BinReader) U16BE() uint16 {
	var bytes []byte
	if r.Err == nil {
		bytes, r.Err = r.c.ReadExactly(2)
	}
	if r.Err != nil {
		return 0
	}
	return binary.BigEndian.Uint16(bytes)
}
func (r *BinReader) U32BE() uint32 {
	var bytes []byte
	if r.Err == nil {
		bytes, r.Err = r.c.ReadExactly(4)
	}
	if r.Err != nil {
		return 0
	}
	return binary.BigEndian.Uint32(bytes)
}
func (r *BinReader) I32BE() int32 {
	return int32(r.U32BE())
}
func (r *BinReader) Bytes(n int) []byte {
	var bytes []byte
	if r.Err == nil {
		bytes, r.Err = r.c.ReadExactly(n)
	}
	if r.Err != nil {
		return nil
	}
	return bytes
}

type BinWriter struct {
	Buf []byte
}

func (r *BinWriter) U8(x uint8) {
	r.Buf = append(r.Buf, x)
}
func (r *BinWriter) U16BE(x uint16) {
	r.Buf = binary.BigEndian.AppendUint16(r.Buf, x)
}
func (r *BinWriter) U32BE(x uint32) {
	r.Buf = binary.BigEndian.AppendUint32(r.Buf, x)
}
func (r *BinWriter) Bytes(x []byte) {
	r.Buf = append(r.Buf, x...)
}

func ConnectTcp(addr string) (*TcpClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewTcpClient(conn, conn.LocalAddr().String()), nil
}

func ServeTcp(cb func(*TcpClient) error) {
	log := Logger{""}
	nextId := 1
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(PORT))
	log.CheckFatal(err)
	defer listener.Close()
	log.Info("Server running on port %d", PORT)
	for {
		conn, err := listener.Accept()
		log.CheckFatal(err)
		id := nextId
		nextId++
		go func(conn net.Conn, id int) {
			defer conn.Close()
			client := NewTcpClient(conn, fmt.Sprintf("peer%d", id))
			client.Log.Info("Connected")
			err := cb(client)
			if err != nil {
				client.Log.Error("Error: %v", err)
			} else {
				client.Log.Info("Done")
			}
			client.Close()
		}(conn, id)
	}
}

type UdpClient struct {
	Msgs chan []byte
	Log  Logger

	conn         net.PacketConn
	addr         net.Addr
	lastActivity time.Time
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

func ServeUdp(timeout time.Duration, cb func(*UdpClient) error) {
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
			t := c.lastActivity.Add(timeout)
			if dl_addr == "" || t.Compare(dl_time) == -1 {
				dl_addr = addr
				dl_time = t
			}
		}
		conn.SetReadDeadline(dl_time)

		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				close(clients[dl_addr].Msgs)
				delete(clients, dl_addr)
				continue
			}
			log.CheckFatal(err)
		}

		cid := addr.String()
		c, has := clients[cid]
		if !has {
			c = &UdpClient{make(chan []byte, UDP_BUFFER), Logger{cid}, conn, addr, time.Now()}
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
		c.lastActivity = time.Now()
		c.Msgs <- msg
	}
}
