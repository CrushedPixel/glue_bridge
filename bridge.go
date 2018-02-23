package glue_bridge

import (
	"encoding/json"
	"github.com/crushedpixel/cement"
	"github.com/crushedpixel/ferry"
	"github.com/crushedpixel/http_bridge"
	"github.com/desertbit/glue"
	"net/http"
	"strings"
	"time"
)

const glueMainChannelName = "m"

type GlueBridge struct {
	server    *glue.Server
	ferry     *ferry.Ferry
	namespace string

	release    chan struct{}
	connecting chan *glue.Socket

	// ConnectionMessageTimeout is the time
	// to wait for the connection message.
	// Defaults to 10s.
	ConnectionMessageTimeout time.Duration
}

type requestPayload struct {
	Method  string `json:"method"`
	Path    string `json:"path"`
	Payload string `json:"payload"`
}

type responsePayload struct {
	Status  int    `json:"status"`
	Payload string `json:"payload"`
}

func Bridge(f *ferry.Ferry, mux *http.ServeMux, namespace string) *GlueBridge {
	namespace = http_bridge.NormalizeNamespace(namespace)
	// the mux pattern must end on a slash to
	// match all subroutes
	pattern := namespace + "/"

	g := glue.NewServer(glue.Options{
		HTTPSocketType: glue.HTTPSocketTypeNone,
		HTTPHandleURL:  namespace,
		CheckOrigin: func(r *http.Request) bool {
			return true // TODO
		},
	})

	// initialize bridge instance
	bridge := &GlueBridge{
		server:    g,
		ferry:     f,
		namespace: namespace,

		release:    make(chan struct{}),
		connecting: make(chan *glue.Socket),

		ConnectionMessageTimeout: 10 * time.Second,
	}
	g.OnNewSocket(bridge.onNewSocket)

	// register glue server with mux
	mux.Handle(pattern, g)

	go bridge.handleConnectingSockets()
	return bridge
}

func (b *GlueBridge) Release() {
	close(b.release)
	b.server.Release()
}

func (b *GlueBridge) onNewSocket(s *glue.Socket) {
	b.connecting <- s
}

func (b *GlueBridge) handleConnectingSockets() {
	for {
		select {
		case socket := <-b.connecting:
			// handle socket connection in goroutine
			// to avoid blocking
			go b.handleConnectingSocket(socket)
		case <-b.release:
			// closing the release channel escapes the for loop
			return
		}
	}
}

func (b *GlueBridge) handleConnectingSocket(socket *glue.Socket) {
	// read connection message
	message, err := socket.Read(b.ConnectionMessageTimeout)
	if err != nil {
		if err != glue.ErrSocketClosed {
			// no connection message received
			socket.Close()
		}
		return
	}

	var payload map[string]string
	err = json.Unmarshal([]byte(message), &payload)
	if err != nil {
		// connection payload was invalid
		socket.Close()
	}

	// convert parsed header object into map[string][]string
	header := make(map[string][]string)
	for key, val := range payload {
		header[key] = []string{val}
	}

	// handle connection request
	cr := &ferry.ConnectionRequest{
		RemoteAddr: socket.RemoteAddr(),
		Header:     header,
	}
	conn, res := b.ferry.NewConnection(cr)
	if res != nil {
		// connection was denied
		socket.Write(marshalResponse(res))
		return
	}

	// connection was successful, initialize socket handler
	b.initSocketConnection(socket, conn)
}

func (b *GlueBridge) initSocketConnection(socket *glue.Socket, conn *ferry.Connection) {
	mainChannel := socket.Channel(glueMainChannelName)
	mainChannel.OnRead(cement.Glue(mainChannel, b.onRequest(conn)))
}

func (b *GlueBridge) onRequest(conn *ferry.Connection) cement.OnReadFunc {
	return func(socket *glue.Socket, messageIdstring, data string) (int, string) {
		payload := &requestPayload{}
		err := json.Unmarshal([]byte(data), payload)
		if err != nil {
			// invalid request payload encountered
			return cement.CodeError, cement.MsgInvalidPayload
		}

		req := &ferry.IncomingRequest{
			Method:     payload.Method,
			RequestURI: payload.Path,
			Payload:    strings.NewReader(payload.Payload),
		}

		res := conn.Handle(req)
		return cement.CodeOk, marshalResponse(res)
	}
}

func marshalResponse(res ferry.Response) string {
	s, p := res.Response()
	payload := &responsePayload{
		Status:  s,
		Payload: p,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	return string(b)
}
