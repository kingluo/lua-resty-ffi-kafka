return function()
    local cnt = ngx.var.arg_cnt or 10000

    require("resty_ffi")
    local kafka = require("resty.ffi_kafka")
    local p = kafka.producer({
        brokers = { "127.0.0.1:9092" },
    })
    local topic = "demo"
    local msg = "foobar"
    ngx.update_time()
    local t1 = ngx.now()
    for _ = 1,cnt do
        assert(p:send(topic, nil, msg))
    end
    ngx.update_time()
    local t2 = ngx.now()
    ngx.say("ffi_kafka: ", t2-t1)
    ngx.flush(true)

    local producer = require "resty.kafka.producer"
    local p = producer:new({
        {
            host = "127.0.0.1",
            port = 9092,
        }
    }, {producer_type="sync"})
    ngx.update_time()
    local t1 = ngx.now()
    for _ = 1,cnt do
        local offset, err = p:send(topic, nil, msg)
        assert(offset ~= nil)
        --ngx.say(tonumber(offset), err)
    end
    ngx.update_time()
    local t2 = ngx.now()
    ngx.say("kafka: ", t2-t1)

    return 200
end
