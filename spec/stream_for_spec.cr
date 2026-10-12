# Phase E spec — stream_for/broadcast_to/broadcasting_for mirror the
# gem oracle oracle/stream_for_expected.yml (captured from actioncable
# 8.1.4 with GlobalID::Identification included, app "crails_app"; the
# real-Rails wire form of to_gid_param is base64url("gid://<app>/
# <Class>/<id>") with no padding). Connection-level: request cookies →
# verified Phase-C session, reject_unauthorized_connection! wire frame.

require "spec"
require "http"
require "base64"
require "../src/action_cable_cr"
require "active_record_cr"
require "action_pack_cr"

SF_EXPECTED = begin
  hash = {} of String => String
  File.read("#{__DIR__}/../oracle/stream_for_expected.yml").each_line do |line|
    if line.includes?('\t')
      id, value = line.split('\t', 2)
      hash[id] = value.chomp
    end
  end
  hash
end

TEST_URL = ENV["AC_TEST_URL"]? ||
           "postgres://root@/crails_ac_test?host=/var/run/postgresql"
ActiveRecord::Base.establish_connection(TEST_URL)
ActiveRecord::Base.connection.exec("DELETE FROM rooms")

ActionCable.gid_app = "crails_app"

class Room < ActiveRecord::Base
end

class TestServer < ActionCable::Server
  def initialize
    super(ActionCable::SubscriptionAdapter::Test.new, "/cable")
  end
end

SERVER = TestServer.new
ActionCable.server = SERVER

class FakeConnection
  include ActionCable::ConnectionAPI

  getter identifiers : Array(Symbol) = [:current_user] of Symbol
  getter transmissions = [] of Hash(String | Symbol, ActionCable::CableValue)

  def transmit(cable) : Nil
    @transmissions << cable
  end

  def pubsub : ActionCable::SubscriptionAdapter::Base
    SERVER.pubsub
  end
end

class SupportChatChannel < ActionCable::Channel::Base
  def subscribed
    stream_for Room.find(42)
  end
end

class PlainChannel < ActionCable::Channel::Base
  def subscribed
    stream_for "plain-object"
  end
end

class UnsavedChannel < ActionCable::Channel::Base
  def subscribed
    room = Room.new
    room.id = 7
    stream_for room
  end
end

module Namespaced
  class DeepChannel < ActionCable::Channel::Base
    def subscribed
      stream_for Room.find(9)
    end
  end
end

def rubylize(ary : Array(String)) : String
  ary.inspect
end

# oracle-строки записаны через Ruby #inspect (в кавычках)
def unquote(s : String) : String
  s.starts_with?('"') && s.ends_with?('"') ? s[1..-2] : s
end

describe "stream_for / broadcast_to" do
  before_all do
    ActiveRecord::Base.connection.exec("DELETE FROM rooms")
    ActiveRecord::Base.connection.exec(
      "INSERT INTO rooms (id, name) VALUES (42, 'general'), (9, 'deep')")
  end

  it "mirrors channel names" do
    SupportChatChannel.channel_name.should eq "support_chat"
    PlainChannel.channel_name.should eq "plain"
    Namespaced::DeepChannel.channel_name.should eq "namespaced:deep"
  end

  it "streams for a persisted AR record (gid)" do
    conn = FakeConnection.new
    channel = SupportChatChannel.new(conn, "{id: 1}")
    channel.subscribe_to_channel
    rubylize(channel.stream_names).should eq SF_EXPECTED["sf_stream_ar"]
  end

  it "streams for a plain object" do
    conn = FakeConnection.new
    channel = PlainChannel.new(conn, "{id: 2}")
    channel.subscribe_to_channel
    rubylize(channel.stream_names).should eq SF_EXPECTED["sf_stream_plain"]
  end

  it "streams for an unsaved AR record" do
    conn = FakeConnection.new
    channel = UnsavedChannel.new(conn, "{id: 4}")
    channel.subscribe_to_channel
    rubylize(channel.stream_names).should eq SF_EXPECTED["sf_stream_unsaved"]
  end

  it "streams for a namespaced channel" do
    conn = FakeConnection.new
    channel = Namespaced::DeepChannel.new(conn, "{id: 3}")
    channel.subscribe_to_channel
    rubylize(channel.stream_names).should eq SF_EXPECTED["sf_stream_namespaced"]
  end

  it "class-level broadcasting_for matches the gem" do
    SupportChatChannel.broadcasting_for(Room.find(42)).should eq unquote(SF_EXPECTED["sf_broadcasting_for"])
    PlainChannel.broadcasting_for("plain-object").should eq unquote(SF_EXPECTED["sf_broadcasting_plain"])
  end

  it "broadcast_to lands on the same pubsub stream the channel listens to" do
    conn = FakeConnection.new
    channel = SupportChatChannel.new(conn, "{id: 1}")
    channel.subscribe_to_channel

    SupportChatChannel.broadcast_to(Room.find(42), {"text" => "hi"})
    adapter = SERVER.pubsub.as(ActionCable::SubscriptionAdapter::Test)
    adapter.broadcasts(SupportChatChannel.broadcasting_for(Room.find(42)))
      .should eq ["{\"text\":\"hi\"}"]
  end

  it "gid param is unpadded base64url of gid://app/Class/id" do
    Base64.urlsafe_encode("gid://crails_app/Room/42", padding: false)
      .should eq unquote(SF_EXPECTED["sf_gid_param"])
  end

  it "stop_stream_for removes the stream" do
    conn = FakeConnection.new
    channel = SupportChatChannel.new(conn, "{id: 1}")
    channel.subscribe_to_channel
    channel.stop_stream_for(Room.find(42))
    channel.stream_names.should be_empty
  end
end

describe "connection session + rejection" do
  it "reads the verified Phase-C session cookie off the upgrade request" do
    # sign a session via the action_pack jar, then replay the cookie
    ActionController.cookies_secret_key_base = "ac-test-secret-key-base"
    io = IO::Memory.new
    response = HTTP::Server::Response.new(io)
    ctx = HTTP::Server::Context.new(
      HTTP::Request.new("GET", "/cable"), response)
    cookies = ActionController::Cookies.new(ctx)
    cookies.signed.set(ActionController::Session::KEY,
      {"warden.user.user.key" => "[[7],\"salt\""})

    wire = response.headers.get("Set-Cookie").join
      .split("\n").find(&.starts_with?("crails_session=")).not_nil!
      .split(";").first

    upgrade = HTTP::Request.new("GET", "/cable")
    upgrade.headers["Cookie"] = wire
    conn = ActionCable::Connection.new(IO::Memory.new, SERVER, upgrade)
    conn.session["warden.user.user.key"].should contain "[[7]"
  end

  it "reject_unauthorized_connection! sends disconnect(unauthorized, false)" do
    io = IO::Memory.new
    conn = ActionCable::Connection.new(io, SERVER).as(ActionCable::Connection)

    # connect hook that rejects
    conn2 = RejectedConnection.new(io, SERVER)
    conn2.run
    frame = io.to_s
    frame.should contain("\"type\":\"disconnect\"")
    frame.should contain("\"reason\":\"unauthorized\"")
    frame.should contain("\"reconnect\":false")
  end
end

class RejectedConnection < ActionCable::Connection
  def connect
    reject_unauthorized_connection!
  end
end
