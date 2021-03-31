package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"time"
)

//ErrBk 非网络问题
var ErrBk error = errors.New("ErrBk")

//ErrNet 网络问题
var ErrNet error = errors.New("ErrNet")

//Conn beanstalkd接口
type Conn interface {
	Use(tube string) (err error)

	Watch(tube string) (id uint, err error)

	Put(body []byte, pri int, delay, ttr time.Duration) (id string, err error)

	Delete(id uint64) error

	Reserve(timeout time.Duration) (id uint64, body []byte, err error)

	Close() (err error)

	Error() (err error)

	Upt(b bool) time.Time
}

type conn struct {
	tc      *textproto.Conn
	created time.Time
	t       time.Time
	err     error
}

func (c *conn) Use(tube string) (err error) {
	err = c.tc.PrintfLine("use " + tube)
	if err != nil {
		c.err = fmt.Errorf("Use %w: %v", ErrNet, err.Error())
		return
	}
	l, err := c.tc.ReadLine()
	if err != nil {
		c.err = fmt.Errorf("Use %w: %v", ErrNet, err.Error())
		return
	}
	if _, err = fmt.Sscanf(l, "USING %s", &tube); err != nil {
		c.err = fmt.Errorf("Use %w: %v", ErrBk, l)
	}
	return
}

func (c *conn) Watch(tube string) (id uint, err error) {
	err = c.tc.PrintfLine("watch " + tube)
	if err != nil {
		c.err = fmt.Errorf("Watch %w: %v", ErrNet, err.Error())
		return
	}
	l, err := c.tc.ReadLine()
	if err != nil {
		c.err = fmt.Errorf("Watch %w: %v", ErrNet, err.Error())
		return
	}
	if _, err = fmt.Sscanf(l, "WATCHING %d", &id); err != nil {
		c.err = fmt.Errorf("Watch %w: %v", ErrBk, l)
	}
	return
}

func (c *conn) Put(body []byte, pri int, delay, ttr time.Duration) (id string, err error) {
	err = c.tc.PrintfLine("put %d %d %d %d\r\n%s", pri, int(delay.Seconds()), int(ttr.Seconds()), len(body), string(body))
	if err != nil {
		c.err = fmt.Errorf("Put %w: %v", ErrNet, err.Error())
		return
	}
	l, err := c.tc.ReadLine()
	if err != nil {
		c.err = fmt.Errorf("Put %w: %v", ErrNet, err.Error())
		return
	}
	if _, err = fmt.Sscanf(l, "INSERTED %s", &id); err != nil {
		c.err = fmt.Errorf("Put %w: %v", ErrBk, l)
	}
	return
}

func (c *conn) Delete(id uint64) (err error) {
	err = c.tc.PrintfLine("delete %d", id)
	if err != nil {
		c.err = fmt.Errorf("Delete %w: %v", ErrNet, err.Error())
		return
	}
	l, err := c.tc.ReadLine()
	if err != nil {
		c.err = fmt.Errorf("Delete %w: %v", ErrNet, err.Error())
		return
	}
	if l != "DELETED" {
		c.err = fmt.Errorf("Delete %w: %v", ErrBk, l)
	}
	return
}

func (c *conn) Reserve(timeout time.Duration) (id uint64, body []byte, err error) {
	err = c.tc.PrintfLine(`reserve-with-timeout %d`, int(timeout.Seconds()))
	if err != nil {
		c.err = fmt.Errorf("Reserve %w: %v", ErrNet, err.Error())
		return
	}
	l, err := c.tc.ReadLine()
	if err != nil {
		c.err = fmt.Errorf("Reserve %w: %v", ErrNet, err.Error())
		return
	}
	var ls uint32
	if _, err = fmt.Sscanf(l, "RESERVED %d %d", &id, &ls); err != nil {
		c.err = fmt.Errorf("Reserve %w: %v", ErrBk, l)
		return
	}
	body = make([]byte, ls+2)
	if _, err = io.ReadFull(c.tc.R, body); err != nil {
		c.err = fmt.Errorf("Reserve %w: %v", ErrNet, err.Error())
		return
	}
	body = body[:ls]
	return
}

func (c *conn) Close() (err error) {
	return c.tc.Close()
}

func (c *conn) Error() (err error) {
	return c.err
}

func (c *conn) Upt(b bool) time.Time {
	if b {
		c.t = time.Now()
		c.err = nil
	}
	return c.t
}

type errConn struct {
	err error
}

func (c *errConn) Use(tube string) (err error) {
	return c.err
}

func (c *errConn) Watch(tube string) (id uint, err error) {
	return id, c.err
}

func (c *errConn) Put(body []byte, pri int, delay, ttr time.Duration) (id string, err error) {
	return id, c.err
}

func (c *errConn) Delete(id uint64) (err error) {
	return c.err
}

func (c *errConn) Reserve(timeout time.Duration) (id uint64, body []byte, err error) {
	return id, body, c.err
}

func (c *errConn) Close() (err error) {
	return nil
}

func (c *errConn) Error() (err error) {
	return c.err
}

func (c *errConn) Upt(b bool) time.Time {
	return time.Now()
}

//Dial 简便方法
func Dial(network, address string) (Conn, error) {
	dialer := &net.Dialer{
		Timeout:   time.Second * 5,
		KeepAlive: time.Minute * 3,
	}
	netConn, err := dialer.Dial(network, address)
	if err != nil {
		return nil, fmt.Errorf("Dial %w: %v", ErrNet, err.Error())
	}
	n := time.Now()
	c := &conn{
		created: n,
		t:       n,
		tc:      textproto.NewConn(netConn),
	}
	return c, nil
}
