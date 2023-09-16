// Package wspb provides helpers for reading and writing protobuf messages.
package wspb // import "github.com/seanpfeifer/websocket/wspb"

import (
	"bytes"
	"context"
	"fmt"

	"github.com/seanpfeifer/websocket"
	"github.com/seanpfeifer/websocket/internal/bpool"
	"github.com/seanpfeifer/websocket/internal/errd"
	"google.golang.org/protobuf/proto"
)

var pbUnmarshalOpts = proto.UnmarshalOptions{DiscardUnknown: true}
var pbMarshalOpts = proto.MarshalOptions{AllowPartial: true}

// Read reads a protobuf message from c into v.
// It will reuse buffers in between calls to avoid allocations.
func Read(ctx context.Context, c *websocket.Conn, v proto.Message) error {
	return read(ctx, c, v)
}

func read(ctx context.Context, c *websocket.Conn, v proto.Message) (err error) {
	defer errd.Wrap(&err, "failed to read protobuf message")

	typ, r, err := c.Reader(ctx)
	if err != nil {
		return err
	}

	if typ != websocket.MessageBinary {
		c.Close(websocket.StatusUnsupportedData, "expected binary message")
		return fmt.Errorf("expected binary message for protobuf but got: %v", typ)
	}

	b := bpool.Get()
	defer bpool.Put(b)

	_, err = b.ReadFrom(r)
	if err != nil {
		return err
	}

	err = pbUnmarshalOpts.Unmarshal(b.Bytes(), v)
	if err != nil {
		c.Close(websocket.StatusInvalidFramePayloadData, "failed to unmarshal protobuf")
		return fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	return nil
}

// Write writes the protobuf message v to c.
// It will reuse buffers in between calls to avoid allocations.
func Write(ctx context.Context, c *websocket.Conn, v proto.Message) error {
	return write(ctx, c, v)
}

func write(ctx context.Context, c *websocket.Conn, v proto.Message) (err error) {
	defer errd.Wrap(&err, "failed to write protobuf message")

	b := bpool.Get()
	pbuf := b.Bytes()
	pbuf, err = pbMarshalOpts.MarshalAppend(pbuf, v)
	defer func() {
		bpool.Put(bytes.NewBuffer(pbuf))
	}()

	if err != nil {
		return fmt.Errorf("failed to marshal protobuf: %w", err)
	}

	return c.Write(ctx, websocket.MessageBinary, pbuf)
}
