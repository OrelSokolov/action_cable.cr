# Oracle cases for ActionCable (Iteration 4).
#
# Run against the ORIGINAL gem:
#   ruby oracle/cases.rb > oracle/expected.yml
#
# Each line: "<case_id>\t<value>". The same case IDs are mirrored 1:1
# in spec/oracle_spec.cr; the Crystal port must reproduce expected.yml
# exactly (captured from actioncable 8.1.4).
#
# Channel behavior is exercised through a minimal fake connection
# (the gem's own unit-test seam: identifiers/transmit/server/config/
# pubsub). Note: subscription confirmations in 8.1 are sent by the
# CONNECTION layer, not the channel — so channel-level transmissions
# stay empty here; the wire protocol is covered by the INTERNAL
# constant cases below.
gem "actioncable", "= 8.1.4"
gem "activesupport", "= 8.1.4"
gem "actionpack", "= 8.1.4"
gem "rack", "~> 3"
gem "nio4r"
gem "concurrent-ruby"
gem "websocket-driver"
gem "rackup"
require "action_cable"
require "active_support/core_ext/hash/indifferent_access"
require "logger"
require "json"

ActionCable.server.config.cable = {"adapter" => "test"}
ActionCable.server.config.logger = Logger.new(File::NULL)

def render(v)
  case v
  when String then v
  else v.inspect
  end
end

def case_run(id)
  puts "#{id}\t#{render(yield)}"
rescue StandardError => e
  puts "#{id}\traise:#{e.class.name}"
end

SERVER = ActionCable::Server::Base.new
SERVER.config.cable = {"adapter" => "test"}

class FakeConnection
  attr_reader :identifiers, :transmissions

  def initialize
    @identifiers = [:current_user]
    @transmissions = []
  end

  def transmit(cable); @transmissions << cable; end
  def server; SERVER; end
  def config; SERVER.config; end
  def pubsub; SERVER.pubsub; end
  def logger; ActiveSupport::Logger.new(File::NULL); end
  def env; {}; end
end

LOG = []

class ChatChannel < ActionCable::Channel::Base
  def subscribed
    stream_from "chat_#{params[:room]}"
    LOG << "subscribed:#{params[:room]}"
  end

  def unsubscribed
    LOG << "unsubscribed"
  end

  def speak(data)
    LOG << "speak:#{data["message"]}"
  end
end

CONN = FakeConnection.new
# Real subscription flow passes ActiveSupport::JSON.decode(identifier)
#   .with_indifferent_access (connection/subscriptions.rb#add)
CH = ChatChannel.new(CONN, "{id: 1}", {"room" => "general"}.with_indifferent_access)

# --- channel ------------------------------------------------------
case_run("ch_identifier") { CH.identifier }
case_run("ch_params")     { CH.params }
case_run("ch_subscribe_callbacks") do
  CH.subscribe_to_channel
  LOG.dup
end
case_run("ch_streams") { CH.send(:streams).keys }
case_run("ch_perform_action") do
  CH.perform_action({"action" => "speak", "message" => "hi"})
  LOG.dup
end
case_run("ch_transmit") do
  # Channel#transmit is private in 8.1 — the connection layer calls it
  CH.send(:transmit, {"hello" => "world"})
  CONN.transmissions.dup
end
case_run("ch_broadcast_recorded") do
  SERVER.pubsub.broadcast("chat_general", {"a" => 1}.to_json)
  SERVER.pubsub.broadcasts("chat_general")
end
case_run("ch_unsubscribe") do
  CH.unsubscribe_from_channel
  LOG.dup
end
case_run("ch_streams_after_unsubscribe") do
  CH.send(:streams).keys
end

# --- wire protocol constants (public, protocol-stable) -------------
case_run("proto_message_types") { ActionCable::INTERNAL[:message_types] }
case_run("proto_disconnect_reasons") { ActionCable::INTERNAL[:disconnect_reasons] }
case_run("proto_default_mount_path") { ActionCable::INTERNAL[:default_mount_path] }
case_run("proto_protocols") { ActionCable::INTERNAL[:protocols] }

# --- remote connections (deterministic surface; runtime WS delivery is
# --- covered by spec/ws_spec.cr / spec/remote_disconnect_spec.cr) -----
class AppConnection < ActionCable::Connection::Base
  identified_by :current_user, :current_account
end

APP_SERVER = ActionCable::Server::Base.new
APP_SERVER.config.cable = {"adapter" => "test"}
APP_SERVER.config.connection_class = -> { AppConnection }
APP_SERVER.config.logger = Logger.new(File::NULL)

RC = APP_SERVER.remote_connections

case_run("rc_internal_channel") do
  RC.where(current_user: "u_1", current_account: "ac_1").send(:internal_channel)
end
case_run("rc_internal_channel_extra_ids") do
  RC.where(current_user: "u_1", current_account: "ac_1", extra: "zzz").send(:internal_channel)
end
case_run("rc_disconnect_payload") do
  RC.where(current_user: "u_1", current_account: "ac_1").disconnect
  APP_SERVER.pubsub.broadcasts("action_cable/ac_1:u_1")
end
case_run("rc_disconnect_no_reconnect") do
  RC.where(current_user: "u_2", current_account: "ac_2").disconnect(reconnect: false)
  APP_SERVER.pubsub.broadcasts("action_cable/ac_2:u_2")
end
case_run("rc_invalid_identifiers") do
  RC.where(current_user: "u_1")
end

# --- periodic timers (registration surface; firing is timing-based and
# --- covered by spec/periodically_spec.cr) ----------------------------
class TickerChannel < ActionCable::Channel::Base
  periodically :tick, every: 5

  periodically every: 2.5 do
    transmit "progress"
  end

  def tick; end
end

case_run("periodic_timers_registered") do
  TickerChannel.periodic_timers.map { |_, options| options[:every].to_f }
end
