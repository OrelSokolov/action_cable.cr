# Oracle spec: the Crystal port must reproduce oracle/expected.yml
# exactly (captured from actioncable 8.1.4 via oracle/cases.rb).
# Same case IDs, same expectations, no edits on either side.

require "spec"
require "../src/action_cable_cr"

EXPECTED = begin
  hash = {} of String => String
  current_id : String? = nil
  File.read("#{__DIR__}/../oracle/expected.yml").each_line do |line|
    if line.includes?('\t')
      id, value = line.split('\t', 2)
      hash[id] = value.chomp
      current_id = id
    elsif cur = current_id
      hash[cur] = hash[cur] + "\n" + line.chomp
    end
  end
  hash
end

LOG = [] of String

class FakeConnection
  include ActionCable::ConnectionAPI

  getter pubsub : ActionCable::SubscriptionAdapter::Base = ActionCable::SubscriptionAdapter::Test.new
  getter transmissions = [] of Hash(String | Symbol, ActionCable::CableValue)

  def identifiers : Array(Symbol)
    [:current_user]
  end

  def transmit(cable : Hash(String | Symbol, ActionCable::CableValue)) : Nil
    @transmissions << cable
  end
end

class ChatChannel < ActionCable::Channel::Base
  def subscribed : Nil
    stream_from "chat_#{params["room"]?}"
    LOG << "subscribed:#{params["room"]?}"
  end

  def unsubscribed : Nil
    LOG << "unsubscribed"
  end

  def speak(data : Hash(String, String)) : Nil
    LOG << "speak:#{data["message"]}"
  end
end

CONN = FakeConnection.new
# Real subscription flow passes a HashWithIndifferentAccess built from
# the identifier JSON (connection/subscriptions.rb#add); our params
# surface is a String-keyed hash (documented deviation).
CH = ChatChannel.new(CONN, "{id: 1}", {"room" => "general"})

RI = ActionCable::RubyInspect

# Connection identifiers for the remote-connections oracle cases (the
# gem's AppConnection has identified_by :current_user, :current_account;
# the macro is idempotent, other spec files may register the same names).
class ActionCable::Connection
  identified_by :current_user, :current_account
end

RC_SERVER = ActionCable::Server.new(ActionCable::SubscriptionAdapter::Test.new)
RC_PUBSUB = RC_SERVER.pubsub.as(ActionCable::SubscriptionAdapter::Test)

class TickerChannel < ActionCable::Channel::Base
  periodically :tick, every: 5

  periodically every: 2.5 do
    transmit "progress"
  end

  def tick : Nil
  end
end

PUBSUB_TEST = CONN.pubsub.as(ActionCable::SubscriptionAdapter::Test)

def pubsub_test
  PUBSUB_TEST
end

TRANSMIT_PAYLOAD = {"hello" => "world"} of String | Symbol => ActionCable::CableValue

CASES = {
  # --- channel ---
  "ch_identifier"            => -> { CH.identifier },
  "ch_params"                => -> { RI.inspect(CH.params) },
  "ch_subscribe_callbacks"   => -> { CH.subscribe_to_channel; RI.inspect(LOG.dup) },
  "ch_streams"               => -> { RI.inspect(CH.stream_names) },
  "ch_perform_action"        => -> { CH.perform_action({"action" => "speak", "message" => "hi"}); RI.inspect(LOG.dup) },
  "ch_transmit"              => -> { CH.transmit(TRANSMIT_PAYLOAD); RI.inspect(CONN.transmissions.dup) },
  "ch_broadcast_recorded"    => -> { pubsub_test.broadcast("chat_general", %q({"a":1})); RI.inspect(pubsub_test.broadcasts("chat_general")) },
  "ch_unsubscribe"           => -> { CH.unsubscribe_from_channel; RI.inspect(LOG.dup) },
  "ch_streams_after_unsubscribe" => -> { RI.inspect(CH.stream_names) },
  # --- wire protocol constants ---
  "proto_message_types"      => -> { RI.inspect(ActionCable::Internal::MESSAGE_TYPES) },
  "proto_disconnect_reasons" => -> { RI.inspect(ActionCable::Internal::DISCONNECT_REASONS) },
  "proto_default_mount_path" => -> { ActionCable::Internal::DEFAULT_MOUNT_PATH },
  "proto_protocols"          => -> { RI.inspect(ActionCable::Internal::PROTOCOLS) },
  # --- remote connections (mirror of the gem's AppConnection cases) ---
  "rc_internal_channel"          => -> { RC_SERVER.remote_connections.where(current_user: "u_1", current_account: "ac_1").internal_channel },
  "rc_internal_channel_extra_ids" => -> { RC_SERVER.remote_connections.where(current_user: "u_1", current_account: "ac_1", extra: "zzz").internal_channel },
  "rc_disconnect_payload"        => -> { RC_SERVER.remote_connections.where(current_user: "u_1", current_account: "ac_1").disconnect; RI.inspect(RC_PUBSUB.broadcasts("action_cable/ac_1:u_1")) },
  "rc_disconnect_no_reconnect"   => -> { RC_SERVER.remote_connections.where(current_user: "u_2", current_account: "ac_2").disconnect(reconnect: false); RI.inspect(RC_PUBSUB.broadcasts("action_cable/ac_2:u_2")) },
  "rc_invalid_identifiers"       => -> { RC_SERVER.remote_connections.where(current_user: "u_1") },
  # --- periodic timers registration ----------------------------------
  "periodic_timers_registered"   => -> { RI.inspect(TickerChannel.periodic_timers.map(&.every_seconds)) },
}

describe "action_cable oracle (actioncable 8.1.4)" do
  EXPECTED.each do |id, expected|
    it id do
      block = CASES[id]?
      block.should_not be_nil, "case #{id} not mirrored in spec"
      result = begin
        block.not_nil!.call.to_s
      rescue ex : Exception
        "raise:#{ex.class.name}"
      end
      result.strip.should eq(expected.strip), <<-MSG
        #{id}:
          expected: #{expected.inspect}
          got:      #{result.inspect}
        MSG
    end
  end

  it "mirrors every oracle case" do
    missing = EXPECTED.keys - CASES.keys
    extra = CASES.keys - EXPECTED.keys
    missing.should be_empty
    extra.should be_empty
  end
end
