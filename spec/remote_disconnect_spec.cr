# Remote disconnect integration spec: a connection with `identified_by`
# identifiers subscribes to its per-connection internal channel
# ("action_cable/<gid>") through the pubsub adapter; publishing a
# disconnect message there — what Server#remote_connections.where(...).
# disconnect does — makes the connection send the disconnect frame and
# close the WebSocket. Mirrors the gem's RemoteConnections +
# Connection::InternalChannel semantics end-to-end on the Async adapter.

require "spec"
require "http/server"
require "http/web_socket"
require "../src/action_cable_cr"

module RemoteSpec
  # The connect callback runs before the welcome message; tests point
  # the identifier at the user they want to disconnect.
  class_property user_id = "u_1"
end

class ActionCable::Connection
  identified_by :current_user, :current_account

  def connect : Nil
    self.current_user = RemoteSpec.user_id
    self.current_account = "spec"
  end
end

# Client with callbacks registered up front — a late-registered
# on_message/on_close races the run fiber and drops frames.
class RemoteClient
  getter messages : Channel(JSON::Any)
  getter closed : Channel(Bool)

  def initialize(address : Socket::IPAddress)
    @ws = HTTP::WebSocket.new("127.0.0.1", "/cable", address.port)
    @messages = Channel(JSON::Any).new(32)
    @closed = Channel(Bool).new
    @ws.on_message { |msg| @messages.send(JSON.parse(msg)) }
    @ws.on_close { @closed.send(true) }
    spawn { @ws.run }
  end

  def next_message(timeout = 2.seconds) : JSON::Any?
    select
    when msg = @messages.receive
      msg
    when timeout(timeout)
      nil
    end
  end

  def wait_closed(timeout = 2.seconds) : Bool
    select
    when ok = @closed.receive
      ok
    when timeout(timeout)
      false
    end
  end
end

def start_cable_server : Tuple(ActionCable::Server, Socket::IPAddress, HTTP::Server)
  server = ActionCable::Server.new
  http = HTTP::Server.new([server.ws_handler])
  address = http.bind_tcp("127.0.0.1", 0)
  spawn { http.listen }
  {server, address, http}
end

describe "ActionCable remote disconnect" do
  it "disconnects a connection found by identifier" do
    RemoteSpec.user_id = "u_42"
    server, address, http = start_cable_server

    server.connections.size.should eq(0)
    client = RemoteClient.new(address)

    msg = client.next_message
    msg.should_not be_nil
    msg.not_nil!["type"].as_s.should eq("welcome")

    # Registered before the welcome message went out.
    server.connections.size.should eq(1)
    server.connections.first.connection_identifier.should eq("spec:u_42")

    server.remote_connections.where(current_user: "u_42", current_account: "spec").disconnect

    msg = client.next_message
    msg.should_not be_nil
    msg.not_nil!["type"].as_s.should eq("disconnect")
    msg.not_nil!["reason"].as_s.should eq("remote")
    msg.not_nil!["reconnect"].as_bool.should be_true

    client.wait_closed.should be_true

    # Unregistered once the socket closed.
    20.times do
      break if server.connections.empty?
      sleep 0.05.seconds
    end
    server.connections.size.should eq(0)

    http.close
  end

  it "honours reconnect: false (client must not reconnect)" do
    RemoteSpec.user_id = "u_43"
    server, address, http = start_cable_server
    client = RemoteClient.new(address)
    client.next_message.should_not be_nil # welcome

    server.remote_connections.where(current_user: "u_43", current_account: "spec")
      .disconnect(reconnect: false)

    msg = client.next_message
    msg.should_not be_nil
    msg.not_nil!["type"].as_s.should eq("disconnect")
    msg.not_nil!["reason"].as_s.should eq("remote")
    msg.not_nil!["reconnect"].as_bool.should be_false

    client.wait_closed.should be_true
    http.close
  end

  it "disconnects every connection sharing the identifier" do
    RemoteSpec.user_id = "u_44"
    server, address, http = start_cable_server

    client1 = RemoteClient.new(address)
    client2 = RemoteClient.new(address)
    client1.next_message.should_not be_nil
    client2.next_message.should_not be_nil

    server.connections.size.should eq(2)
    server.remote_connections.where(current_user: "u_44", current_account: "spec").disconnect

    [client1, client2].each do |client|
      msg = client.next_message
      msg.not_nil!["type"].as_s.should eq("disconnect")
      client.wait_closed.should be_true
    end

    http.close
  end

  it "raises when not every identifier is provided" do
    server = ActionCable::Server.new
    expect_raises(ActionCable::RemoteConnections::RemoteConnection::InvalidIdentifiersError) do
      server.remote_connections.where(current_user: "u_1")
    end
  end
end
