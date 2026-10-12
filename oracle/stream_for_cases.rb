# Oracle cases for ActionCable stream_for/broadcast_to (Phase E).
# Run against the ORIGINAL gem 8.1.4:
#   ruby oracle/stream_for_cases.rb > oracle/stream_for_expected.yml
#
# broadcasting_for (8.1.4 channel/broadcasting.rb):
#   serialize_broadcasting([channel_name] + Array(broadcastables)) where
#   each element → to_gid_param (GlobalID) when it responds, else
#   to_param; joined with ":". Real Rails apps load globalid (via
# activejob), so AR records serialize as gid://<app>/<class>/<id>;
# here the app name is the default (no config, globalid falls back to
# the process's APP_NAME env or nil → "gid://" shape verified below).
gem "actioncable", "= 8.1.4"
gem "activesupport", "= 8.1.4"
gem "actionpack", "= 8.1.4"
gem "activerecord", "= 8.1.4"
gem "globalid"
gem "rack", "~> 3"
gem "nio4r"
gem "concurrent-ruby"
gem "websocket-driver"
gem "rackup"
gem "sqlite3", ">= 2.1"
require "action_cable"
require "active_record"
require "globalid"
require "logger"

# In a real Rails app the globalid railtie includes GlobalID::
# Identification into ActiveRecord::Base and sets the app name; a bare
# gem harness does not — do it by hand to capture the real wire names.
ActiveRecord::Base.include GlobalID::Identification
GlobalID.app = "crails_app"

ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")
ActiveRecord::Schema.define do
  create_table :rooms do |t|
    t.string :name
  end
end

class Room < ActiveRecord::Base
end

ActionCable.server.config.cable = {"adapter" => "test"}
ActionCable.server.config.logger = Logger.new(File::NULL)

SERVER = ActionCable::Server::Base.new

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
    stream_for Room.new(id: 7)
  end
end

module Namespaced
  class DeepChannel < ActionCable::Channel::Base
    def subscribed
      stream_for Room.find(9)
    end
  end
end

def line(id, value)
  puts "#{id}\t#{value.inspect}"
end

Room.create!(id: 42, name: "general")
Room.create!(id: 9, name: "deep")

conn = FakeConnection.new
channel = SupportChatChannel.new(conn, "{id: 1}", id: 1)
channel.subscribe_to_channel
line("sf_stream_ar", channel.send(:streams).keys)

plain = PlainChannel.new(conn, "{id: 2}")
plain.subscribe_to_channel
line("sf_stream_plain", plain.send(:streams).keys)

unsaved = UnsavedChannel.new(conn, "{id: 4}")
unsaved.subscribe_to_channel
line("sf_stream_unsaved", unsaved.send(:streams).keys)

deep = Namespaced::DeepChannel.new(conn, "{id: 3}")
deep.subscribe_to_channel
line("sf_stream_namespaced", deep.send(:streams).keys)

# broadcast_to target name (the gem's class-level helper)
line("sf_broadcasting_for", SupportChatChannel.broadcasting_for(Room.find(42)))
line("sf_broadcasting_plain", PlainChannel.broadcasting_for("plain-object"))
line("sf_gid_param", Room.find(42).to_gid_param)
