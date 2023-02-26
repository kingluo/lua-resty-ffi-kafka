//
// Copyright (c) 2023, Jinhua Luo (kingluo) luajit.io@gmail.com
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
//    contributors may be used to endorse or promote products derived from
//    this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
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
