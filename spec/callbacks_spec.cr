# on_subscribe / on_unsubscribe callbacks (gem's Channel::Callbacks):
# :after on subscribe, :before on unsubscribe, unless-guard honored.

require "spec"
require "../src/action_cable_cr"

CB_LOG = [] of String

class FakeCallbackConnection
  include ActionCable::ConnectionAPI

  getter pubsub : ActionCable::SubscriptionAdapter::Base = ActionCable::SubscriptionAdapter::Test.new

  def identifiers : Array(Symbol)
    [] of Symbol
  end

  def transmit(cable : Hash(String | Symbol, ActionCable::CableValue)) : Nil
  end
end

class CallbackChannel < ActionCable::Channel::Base
  on_subscribe :present, unless: :subscription_rejected?
  on_unsubscribe :absent, unless: :subscription_rejected?

  def subscribed : Nil
    CB_LOG << "subscribed"
  end

  def unsubscribed : Nil
    CB_LOG << "unsubscribed"
  end

  def present : Nil
    CB_LOG << "present"
  end

  def absent : Nil
    CB_LOG << "absent"
  end
end

class RejectingChannel < ActionCable::Channel::Base
  on_subscribe :present, unless: :subscription_rejected?
  on_unsubscribe :absent, unless: :subscription_rejected?

  def subscribed : Nil
    reject
  end

  def unsubscribed : Nil
    CB_LOG << "unsubscribed-rejecting"
  end

  def present : Nil
    CB_LOG << "present-rejecting"
  end

  def absent : Nil
    CB_LOG << "absent-rejecting"
  end
end

describe "on_subscribe/on_unsubscribe" do
  it "runs subscribe callback after subscribed and unsubscribe callback before unsubscribed" do
    CB_LOG.clear
    channel = CallbackChannel.new(FakeCallbackConnection.new, "{id: 1}")
    channel.subscribe_to_channel
    channel.unsubscribe_from_channel
    CB_LOG.should eq ["subscribed", "present", "absent", "unsubscribed"]
  end

  it "skips callbacks when the subscription was rejected" do
    CB_LOG.clear
    channel = RejectingChannel.new(FakeCallbackConnection.new, "{id: 2}")
    channel.subscribe_to_channel
    channel.subscription_rejected?.should be_true
    channel.unsubscribe_from_channel
    CB_LOG.should eq ["unsubscribed-rejecting"]
  end
end
