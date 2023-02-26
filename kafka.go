package main

/*
#cgo LDFLAGS: -shared
#include <string.h>
void* ngx_http_lua_ffi_task_poll(void *p);
char* ngx_http_lua_ffi_get_req(void *tsk, int *len);
void ngx_http_lua_ffi_respond(void *tsk, int rc, char* rsp, int rsp_len);
*/
import "C"
import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"
	"unsafe"

	kafka "github.com/segmentio/kafka-go"
)

const (
	NEW_PRODUCER int = iota
	CLOSE_PRODUCER
	SEND
)

type WriterOptions struct {
	Brokers []string `json:"brokers,omitempty"`
	Cap     int      `json:"cap"`
}

type Message struct {
	IsBase64 bool    `json:"is_base64,omitempty"`
	Topic    string  `json:"topic,omitempty"`
	Key      *string `json:"key,omitempty"`
	Value    string  `json:"value,omitempty"`
}

type Writer struct {
	Idx      int64          `json:"idx"`
	Options  *WriterOptions `json:"options,omitempty"`
	Messages []Message      `json:"messages,omitempty"`
}

type Request struct {
	task   unsafe.Pointer `json:"-"`
	Cmd    int            `json:"cmd"`
	Writer *Writer        `json:"writer,omitempty"`
}

func run_producer(w *kafka.Writer, ch chan *Request) {
	for req := range ch {
		var msgs []kafka.Message
		for _, m := range req.Writer.Messages {
			msg := kafka.Message{
				Topic: m.Topic,
				Value: []byte(m.Value),
			}
			if m.Key != nil {
				msg.Key = []byte(*m.Key)
			}
			msgs = append(msgs, msg)
		}

		var errs []string
		switch err := w.WriteMessages(context.TODO(), msgs...).(type) {
		case nil:
		case kafka.WriteErrors:
			for i := range msgs {
				if err[i] != nil {
					errs = append(errs, err[i].Error())
				}
			}
		default:
			log.Fatal(err)
		}

		if errs != nil {
			rsp, err := json.Marshal(errs)
			if err != nil {
				log.Fatalln("put failed, err:", err)
			}
			C.ngx_http_lua_ffi_respond(req.task, 1, (*C.char)(C.CBytes(rsp)), C.int(len(rsp)))
		} else {
			C.ngx_http_lua_ffi_respond(req.task, 0, nil, 0)
		}
	}
}

//export libffi_init
func libffi_init(_ *C.char, tq unsafe.Pointer) C.int {
	go func() {
		var idx int64
		writers := make(map[int64]chan *Request)
		for {
			task := C.ngx_http_lua_ffi_task_poll(tq)
			if task == nil {
				log.Println("exit kafka-go runtime")
				break
			}

			var rlen C.int
			r := C.ngx_http_lua_ffi_get_req(task, &rlen)
			data := C.GoBytes(unsafe.Pointer(r), rlen)
			var req Request
			err := json.Unmarshal(data, &req)
			if err != nil {
				log.Fatalln("error:", err)
			}

			switch req.Cmd {
			case NEW_PRODUCER:
				idx += 1
				w := &kafka.Writer{
					Addr:         kafka.TCP(req.Writer.Options.Brokers...),
					RequiredAcks: kafka.RequireOne,
					Balancer:     &kafka.LeastBytes{},
					BatchSize:    1,
					BatchTimeout: 10 * time.Millisecond,
				}
				ch := make(chan *Request, req.Writer.Options.Cap)
				writers[idx] = ch
				go run_producer(w, ch)
				rsp := strconv.FormatInt(idx, 10)
				C.ngx_http_lua_ffi_respond(task, 0, (*C.char)(C.CString(rsp)), C.int(len(rsp)))
			case CLOSE_PRODUCER:
				close(writers[req.Writer.Idx])
				delete(writers, req.Writer.Idx)
				C.ngx_http_lua_ffi_respond(task, 0, nil, 0)
			case SEND:
				req.task = task
				writers[req.Writer.Idx] <- &req
			}
		}
	}()

	return 0
}

func main() {}
