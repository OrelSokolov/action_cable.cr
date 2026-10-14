# Integration spec: real HTTP server + WebSocket client exercising the
# Rails ActionCable JSON protocol end-to-end (welcome → subscribe →
# confirmation → action → stream delivery → ping).

require "spec"
require "http/server"
require "http/web_socket"
require "../src/action_cable_cr"

class BenchChannel < ActionCable::Channel::Base
  def subscribed : Nil
    stream_from "bench"
  end

  def ping(data : Hash(String, String)) : Nil
    transmit({"pong" => data["n"]?} of String | Symbol => ActionCable::CableValue)
  end
end

def next_ws_message(ws : HTTP::WebSocket) : JSON::Any?
  got = Channel(JSON::Any?).new(1)
  ws.on_message { |msg| got.send(JSON.parse(msg)) }
  select
    when msg = got.receive
      msg
    when timeout(2.seconds)
      nil
  end
end

describe "action_cable websocket protocol" do
  it "speaks welcome/confirm/message/broadcast" do
    server = ActionCable::Server.new
    http = HTTP::Server.new([server.ws_handler])
    address = http.bind_tcp("127.0.0.1", 0)
    spawn { http.listen }

    ws = HTTP::WebSocket.new("127.0.0.1", "/cable", address.port)
    spawn { ws.run }

    # Handshake: welcome.
    msg = next_ws_message(ws)
    msg.should_not be_nil
    msg.not_nil!["type"].as_s.should eq("welcome")

    # Subscribe → confirm_subscription (connection layer, per 8.1).
    identifier = %q({"channel":"BenchChannel"})
    ws.send({"command" => "subscribe", "identifier" => identifier}.to_json)
    msg = next_ws_message(ws)
    msg.should_not be_nil
    msg.not_nil!["type"].as_s.should eq("confirm_subscription")
    msg.not_nil!["identifier"].as_s.should eq(identifier)

    # Action dispatch through the wire: channel transmits its reply.
    ws.send(%q({"command":"message","identifier":) + identifier.to_json +
             %q(,"data":"{\"action\":\"ping\",\"n\":\"1\"}"}))
    msg = next_ws_message(ws)
    msg.should_not be_nil
    msg.not_nil!["identifier"].as_s.should eq(identifier)
    msg.not_nil!["message"]["pong"].as_s.should eq("1")

    # Broadcast reaches the subscribed stream.
    server.broadcast("bench", %q({"hello":"world"}))
    msg = next_ws_message(ws)
    msg.should_not be_nil
    msg.not_nil!["message"]["hello"].as_s.should eq("world")

    ws.close
    http.close
  end

  it "echoes the selected websocket subprotocol in the 101 handshake" do
    server = ActionCable::Server.new
    http = HTTP::Server.new([server.ws_handler])
    address = http.bind_tcp("127.0.0.1", 0)
    spawn { http.listen }

    # Клиентские контракты (tungstenite, @rails/actioncable) требуют, чтобы
    # 101-ответ подтвердил под-протокол из предложенных.
    socket = TCPSocket.new("127.0.0.1", address.port)
    socket << "GET /cable HTTP/1.1\r\nHost: 127.0.0.1\r\n" \
              "Upgrade: websocket\r\nConnection: Upgrade\r\n" \
              "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" \
              "Sec-WebSocket-Version: 13\r\n" \
              "Sec-WebSocket-Protocol: actioncable-v1-json, actioncable-unsupported\r\n\r\n"
    response = String.build do |io|
      while (line = socket.gets) && !line.strip.empty?
        io << line
      end
    end
    response.should contain("101")
    response.should contain("Sec-WebSocket-Protocol: actioncable-v1-json")
    socket.close
    http.close
  end
end
