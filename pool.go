package main

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

//Pool ...
type Pool struct {
	// Dial is an application supplied function for creating and configuring a
	// connection.
	//
	// The connection returned from Dial must not be in a special state
	// (subscribed to pubsub channel, transaction started, ...).
	Dial func() (Conn, error)

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// Close connections older than this duration. If the value is zero, then
	// the pool does not close connections based on age.
	MaxConnLifetime time.Duration

	mu       sync.Mutex           // mu protects the following fields
	closed   bool                 // set to true when the pool is closed.
	active   int                  // the number of open connections in the pool
	initOnce sync.Once            // the init ch once func
	idle     *list.List           // idle connections
	wait     map[uint64]chan Conn //wait chan map
	nextWait uint64               // Next key to use in wait chan.
}

var nowFunc = time.Now // for testing
var errPoolClosed = errors.New("pool: connection pool closed")

func (p *Pool) nextWaitKeyLocked() uint64 {
	next := p.nextWait
	p.nextWait++
	return next
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p *Pool) Get(ctx context.Context) (c Conn) {
	p.initOnce.Do(func() {
		p.idle = list.New()
		p.wait = make(map[uint64]chan Conn)
	})
	p.mu.Lock()
	//最快速度找到可用连接
	if p.IdleTimeout > 0 && p.MaxConnLifetime > 0 {
		//前->后
		for p.idle.Len() > 0 {
			ev := p.idle.Remove(p.idle.Front()).(*conn)
			p.mu.Unlock()
			if ev.t.Add(p.IdleTimeout).After(nowFunc()) && nowFunc().Sub(ev.created) < p.MaxConnLifetime {
				return ev
			}
			// log.Println("close3")
			ev.Close()
			p.mu.Lock()
			p.active--
		}
	} else if p.IdleTimeout > 0 {
		//前->后
		for p.idle.Len() > 0 {
			ev := p.idle.Remove(p.idle.Front()).(*conn)
			p.mu.Unlock()
			if ev.t.Add(p.IdleTimeout).After(nowFunc()) {
				return ev
			}
			// log.Println("close3")
			ev.Close()
			p.mu.Lock()
			p.active--
		}
	} else if p.MaxConnLifetime > 0 {
		//前->后
		for p.idle.Len() > 0 {
			ev := p.idle.Remove(p.idle.Front()).(*conn)
			p.mu.Unlock()
			if nowFunc().Sub(ev.created) < p.MaxConnLifetime {
				return ev
			}
			// log.Println("close3")
			ev.Close()
			p.mu.Lock()
			p.active--
		}
	} else if p.idle.Len() > 0 {
		ev := p.idle.Remove(p.idle.Front()).(*conn)
		p.mu.Unlock()
		return ev
	}

	if p.closed {
		p.mu.Unlock()
		return &errConn{err: errPoolClosed}
	}
	//达到最大连接数限制
	if p.MaxActive > 0 && p.active >= p.MaxActive {
		wc := make(chan Conn, 1)
		wk := p.nextWaitKeyLocked()
		p.wait[wk] = wc
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			p.mu.Lock()
			delete(p.wait, wk)
			p.mu.Unlock()
			select {
			default:
			case c, ok := <-wc:
				if ok && c != nil {
					p.Put(c)
				}
			}
			return &errConn{err: ctx.Err()}
		case c, b := <-wc:
			if !b {
				return &errConn{err: errPoolClosed}
			}
			return c
		}
	}
	p.active++
	p.mu.Unlock()
	c, err := p.Dial()
	if err != nil {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
		return &errConn{err: err}
	}
	return
}

//Put put a conn back to idle list
func (p *Pool) Put(c Conn) (err error) {
	if _, b := c.(*errConn); b {
		return nil
	}
	//断开的连接？
	if err = c.Error(); err != nil && errors.Is(err, ErrNet) {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
		// log.Println("close0")
		return c.Close()
	}
	//连接正常->放回	
	c.Upt(true)
	p.mu.Lock()
	//优先->wait chan
	if len(p.wait) > 0 {
		var wc chan Conn
		var k uint64
		for k, wc = range p.wait {
			wc <- c
			break
		}
		delete(p.wait, k)
		p.mu.Unlock()
		return nil
	} 
	//confirm pool not closed
	if p.closed {
		p.active--
		p.mu.Unlock()
		// log.Println("close1")
		return c.Close()
	}
	//put back and check idle count
	p.idle.PushBack(c)
	if p.MaxIdle > 0 && p.idle.Len() > p.MaxIdle {
		c = p.idle.Remove(p.idle.Front()).(Conn)
		p.active--
		p.mu.Unlock()
		// log.Println("close2")
		return c.Close()
	}
	p.mu.Unlock()
	return nil
}

//Close close the pool
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed || p.idle == nil {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.active -= p.idle.Len()
	l := p.idle
	p.idle = list.New()
	for k, v := range p.wait {
		delete(p.wait, k)
		close(v)
	}
	p.mu.Unlock()
	for e := l.Front(); e != nil; e = e.Next() {
		log.Println("[Close conn]", e.Value.(Conn).Close())
	}
	return nil
}

//Stat 打印池子信息
func (p *Pool) Stat() string {
	return fmt.Sprintf("Max=%d, Active=%d, Idel=%d", p.MaxActive, p.active, p.idle.Len())
}

