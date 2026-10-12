# Periodically (Channel::PeriodicTimers port): timers registered via the
# `periodically` class DSL start on subscribe, fire on the interval, and
# stop on unsubscribe — Rails 8.1 semantics on fiber timers instead of
# the gem's event_loop timer + worker pool.

require "spec"
require "../src/action_cable_cr"

module PeriodicLog
  class_property ticks = [] of String
end

class PeriodicConn
  include ActionCable::ConnectionAPI

  getter pubsub : ActionCable::SubscriptionAdapter::Base = ActionCable::SubscriptionAdapter::Test.new
  getter transmissions = [] of Hash(String | Symbol, ActionCable::CableValue)

  def identifiers : Array(Symbol)
    [] of Symbol
  end

  def transmit(cable : Hash(String | Symbol, ActionCable::CableValue)) : Nil
    @transmissions << cable
  end
end

class MethodTickerChannel < ActionCable::Channel::Base
  periodically :tick, every: 0.05

  def tick : Nil
    PeriodicLog.ticks << "method"
  end
end

class BlockTickerChannel < ActionCable::Channel::Base
  periodically every: 0.05 do
    PeriodicLog.ticks << "block"
  end
end

class InheritedTickerChannel < MethodTickerChannel
  periodically :tock, every: 0.05

  def tock : Nil
    PeriodicLog.ticks << "tock"
  end
end

class ZeroTickerChannel < ActionCable::Channel::Base
  periodically :never, every: 0.seconds
end

describe "ActionCable::Channel periodically" do
  it "fires a method timer while subscribed" do
    PeriodicLog.ticks.clear
    channel = MethodTickerChannel.new(PeriodicConn.new, "{}")
    channel.subscribe_to_channel
    sleep 0.3.seconds
    channel.unsubscribe_from_channel

    fired = PeriodicLog.ticks.count("method")
    fired.should be >= 3 # 0.05s interval over a 0.3s window

    sleep 0.2.seconds
    PeriodicLog.ticks.count("method").should eq(fired) # stopped
  end

  it "fires a block timer (channel as self)" do
    PeriodicLog.ticks.clear
    channel = BlockTickerChannel.new(PeriodicConn.new, "{}")
    channel.subscribe_to_channel
    sleep 0.3.seconds
    channel.unsubscribe_from_channel

    fired = PeriodicLog.ticks.count("block")
    fired.should be >= 3

    sleep 0.2.seconds
    PeriodicLog.ticks.count("block").should eq(fired)
  end

  it "inherits timers from the parent channel class" do
    ticks = InheritedTickerChannel.periodic_timers.map(&.method_name).sort
    ticks.should eq(["tick", "tock"])
  end

  it "registers timers with their intervals (oracle mirror)" do
    MethodTickerChannel.periodic_timers.map(&.every_seconds).should eq([0.05])
  end

  it "rejects a non-positive interval like the gem" do
    expect_raises(ArgumentError) do
      ZeroTickerChannel.periodic_timers
    end
  end
end
