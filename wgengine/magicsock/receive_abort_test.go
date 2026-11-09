// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package magicsock

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"runtime"
	"slices"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tailscale/wireguard-go/conn"
	"tailscale.com/health"
	"tailscale.com/types/nettype"
	"tailscale.com/util/eventbus/eventbustest"
)

// scriptedPacketConn returns scripted errors; nil yields an empty datagram.
type scriptedPacketConn struct {
	errors []error
	calls  int
}

func (p *scriptedPacketConn) ReadFromUDPAddrPort([]byte) (int, netip.AddrPort, error) {
	err := p.errors[p.calls]
	p.calls++
	return 0, netip.AddrPort{}, err
}

func (*scriptedPacketConn) WriteToUDPAddrPort([]byte, netip.AddrPort) (int, error) {
	return 0, nil
}
func (*scriptedPacketConn) Close() error                     { return nil }
func (*scriptedPacketConn) SetDeadline(time.Time) error      { return nil }
func (*scriptedPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (*scriptedPacketConn) SetWriteDeadline(time.Time) error { return nil }
func (*scriptedPacketConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }

var _ nettype.PacketConn = (*scriptedPacketConn)(nil)

func TestReceiveFuncRetriesRecoverableErrors(t *testing.T) {
	for _, tt := range []struct {
		err      error
		feedback bool
	}{
		{syscall.EHOSTUNREACH, runtime.GOOS != "windows"},
		{syscall.ENETUNREACH, runtime.GOOS != "windows"},
		{syscall.Errno(10065), runtime.GOOS == "windows"},
		{syscall.Errno(10051), runtime.GOOS == "windows"},
		{syscall.EADDRNOTAVAIL, false},
		{syscall.Errno(10049), false}, // WSAEADDRNOTAVAIL
		{syscall.EBADF, false},
		{syscall.ENOTSOCK, false},
		{syscall.ECONNRESET, false},
		{syscall.ECONNREFUSED, false},
		{errors.New("unexpected error"), false},
	} {
		t.Run(tt.err.Error(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				wrapped := &net.OpError{Op: "read", Net: "udp4", Err: &os.SyscallError{
					Syscall: "recvfrom", Err: tt.err,
				}}
				pconn := &scriptedPacketConn{}
				for range 12 {
					pconn.errors = append(pconn.errors, wrapped)
				}
				pconn.errors = append(pconn.errors, nil, wrapped, fmt.Errorf("read: %w", net.ErrClosed))
				c := newConn(t.Logf)
				c.connCtx, c.connCtxCancel = context.WithCancel(t.Context())
				t.Cleanup(c.connCtxCancel)
				c.pconn4.setConnLocked([]nettype.PacketConn{pconn}, 1, "udp4", nil)
				recv := c.mkReceiveFunc(udpReceiveConfig{ruc: &c.pconn4, family: "ReceiveIPv4-test"})
				slab := make([]byte, 1280)
				packets := make([]conn.ReceivedPacket, 1)
				before := metricRecvUDPError.Value()
				start := time.Now()
				n, err := recv(slab, packets)
				if n != 0 || !errors.Is(err, net.ErrClosed) {
					t.Fatalf("got (%d, %v), want (0, net.ErrClosed)", n, err)
				}
				if pconn.calls != len(pconn.errors) {
					t.Fatalf("reads = %d, want %d", pconn.calls, len(pconn.errors))
				}
				if got := metricRecvUDPError.Value() - before; got != 13 {
					t.Errorf("error metric grew by %d, want 13", got)
				}
				// Only non-feedback errors wait before retrying.
				if waited := time.Since(start) > 0; waited == tt.feedback {
					t.Errorf("waited = %v, want %v", waited, !tt.feedback)
				}
				c.connCtxCancel()
				if _, err := recv(slab, packets); !errors.Is(err, net.ErrClosed) {
					t.Fatalf("error after cancellation = %v, want net.ErrClosed", err)
				}
			})
		})
	}
}

var errReadClosed = fmt.Errorf("read: %w", net.ErrClosed)

func readErr(errno error) error {
	return &net.OpError{Op: "read", Net: "udp4", Err: &os.SyscallError{Syscall: "recvfrom", Err: errno}}
}

func recv(fn conn.ReceiveFunc) error {
	_, err := fn(make([]byte, 1280), make([]conn.ReceivedPacket, 1))
	return err
}

// newScriptedRecv returns a minimal Conn and an IPv4 receive func reading
// from pconn.
func newScriptedRecv(t *testing.T, pconn *scriptedPacketConn) (*Conn, conn.ReceiveFunc) {
	c := newConn(t.Logf)
	c.connCtx, c.connCtxCancel = context.WithCancel(t.Context())
	t.Cleanup(c.connCtxCancel)
	c.health = health.NewTracker(eventbustest.NewBus(t))
	c.pconn4.setConnLocked([]nettype.PacketConn{pconn}, 1, "udp4", nil)
	return c, c.mkReceiveFunc(udpReceiveConfig{ruc: &c.pconn4, family: "ReceiveIPv4-test"})
}

func TestReceiveFuncCloseInterruptsRetryDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, recvFn := newScriptedRecv(t, &scriptedPacketConn{errors: slices.Repeat([]error{readErr(syscall.ECONNRESET)}, 100)})
		done := make(chan error, 1)
		go func() { done <- recv(recvFn) }()

		time.Sleep(10*readErrRetryDelay + readErrRetryDelay/2) // mid-way through a retry delay
		c.connCtxCancel()
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, net.ErrClosed) {
				t.Fatalf("recv = %v, want net.ErrClosed", err)
			}
		default:
			t.Fatal("recv still blocked after close")
		}
	})
}

func TestReceiveFuncBrokenHealth(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := readErr(syscall.ECONNRESET)
		var script []error
		script = append(script, e, e, e, errReadClosed) // call 1: a few errors
		// call 2: over readErrBrokenAfter of retries
		script = append(script, slices.Repeat([]error{e}, int(readErrBrokenAfter/readErrRetryDelay)+1)...)
		script = append(script, errReadClosed, nil, errReadClosed) // call 3: a successful read
		c, recvFn := newScriptedRecv(t, &scriptedPacketConn{errors: script})
		stats := c.health.ReceiveFuncStats("ReceiveIPv4-test")

		for i, want := range []bool{false, true, false} {
			recv(recvFn)
			if got := stats.Broken(); got != want {
				t.Errorf("after call %d: Broken = %v, want %v", i+1, got, want)
			}
		}

		stats.SetBroken(true)
		c.mkReceiveFunc(udpReceiveConfig{ruc: &c.pconn4, family: "ReceiveIPv4-test"})
		if stats.Broken() {
			t.Error("still broken after a new receive func")
		}
	})
}
