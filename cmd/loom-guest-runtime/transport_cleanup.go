package main

import (
	"net/http"

	"github.com/qianyi-sun/loom/internal/guestchannel"
)

func closeGuestTransport(channel *guestchannel.Client, transport *http.Transport) {
	// Interrupt transport I/O before closing HTTP streams: a stream's FIN can
	// otherwise wait for the stalled guest before the channel is ever closed.
	_ = channel.Close()
	transport.CloseIdleConnections()
}
