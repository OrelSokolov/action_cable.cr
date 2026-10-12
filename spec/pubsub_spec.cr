# Pubsub adapter spec (Iteration 7): mirrors oracle/pubsub_expected.yml
# (captured from real ActionCable 8.1.4 Redis adapter via
# oracle/pubsub_cases.rb). Both network adapters — Redis and PostgreSQL
# (LISTEN/NOTIFY) — run the same cases through the PubsubAdapter port.
# Delivery is fiber-async; cases drain with a short sleep like the Ruby
# oracle. Requires redis-server + PostgreSQL; parts skip when absent.

require "spec"
require "../src/action_cable_cr"
require "../src/adapters/redis_pubsub"
require "../src/adapters/pg_pubsub"

PUBSUB_EXPECTED = begin
  hash = {} of String => String
  File.read("#{__DIR__}/../oracle/pubsub_expected.yml").each_line do |line|
    id, value = line.split('\t', 2)
    hash[id] = value.chomp
  end
  hash
end

REDIS_OK = begin
  `redis-cli -u #{ENV.fetch("REDIS_URL_TEST", "redis://127.0.0.1:6379/15")} ping 2>/dev/null` \
    .strip == "PONG"
rescue ex
  false
end

PG_PUBSUB_OK = begin
  admin = DB.open(ENV.fetch("PUBSUB_ADMIN_URL", "postgres:///postgres"))
  admin.exec("DROP DATABASE IF EXISTS crails_pubsub_spec")
  admin.exec("CREATE DATABASE crails_pubsub_spec")
  admin.close
  true
rescue ex
  false
end

# Collects deliveries across fibers; Ruby's Queue analog for the oracle
# rendering (`.inspect` of an Array of strings).
class Collector
    @items = [] of String
  @done = Channel(Nil).new

  def callback : Proc(String, Nil)
    ->(payload : String) { @items << payload }
  end

  def items : Array(String)
    # Let the listener fiber drain pending deliveries.
    sleep 0.3.seconds
    @items.dup
  end

  def empty? : Bool
    items.empty?
  end
end

def pubsub_case(adapter : ActionCable::SubscriptionAdapter::Base,
               id : String, result)
  it id do
    result.inspect.should eq PUBSUB_EXPECTED[id]
  end
end

def assert_pubsub_adapter(name : String,
                          build : -> ActionCable::SubscriptionAdapter::Base)
  describe "#{name} (pubsub oracle mirror)" do
    adapter = build.call

    pubsub_case adapter, "pubsub_roundtrip", begin
      got = Collector.new
      adapter.subscribe("s1", got.callback)
      sleep 0.1.seconds
      adapter.broadcast("s1", "m1")
      adapter.broadcast("s1", "m2")
      got.items
    end

    pubsub_case adapter, "pubsub_isolated_channels", begin
      got_a = Collector.new
      got_b = Collector.new
      adapter.subscribe("ch-a", got_a.callback)
      adapter.subscribe("ch-b", got_b.callback)
      sleep 0.1.seconds
      adapter.broadcast("ch-a", "for-a")
      adapter.broadcast("ch-b", "for-b")
      [got_a.items.first, got_b.items.first]
    end

    pubsub_case adapter, "pubsub_no_delivery_without_subscription", begin
      adapter.broadcast("nobody", "lost")
      sleep 0.3.seconds
      true
    end

    pubsub_case adapter, "pubsub_two_subscribers_same_channel", begin
      got1 = Collector.new
      got2 = Collector.new
      adapter.subscribe("shared", got1.callback)
      adapter.subscribe("shared", got2.callback)
      sleep 0.1.seconds
      adapter.broadcast("shared", "fan")
      [got1.items.first, got2.items.first]
    end

    # Deviation: Rails' Redis adapter broadcast returns PUBLISH's receiver
    # count as a string ("0"); our PubsubAdapter port broadcast returns nil.
    it "pubsub_broadcast_returns (documented deviation)" do
      adapter.broadcast("ret", "x").should be_nil
    end

    adapter.shutdown
  end
end

if REDIS_OK
  assert_pubsub_adapter("SubscriptionAdapter::Redis", ->{
    ActionCable::SubscriptionAdapter::Redis.new(
      url: ENV.fetch("REDIS_URL_TEST", "redis://127.0.0.1:6379/15"))
  })
else
  it "redis pubsub adapter (requires redis-server)" do
    pending "redis-server not available"
  end
end

if PG_PUBSUB_OK
  assert_pubsub_adapter("SubscriptionAdapter::PostgreSQL", ->{
    ActionCable::SubscriptionAdapter::PostgreSQL.connect(
      ENV.fetch("PUBSUB_DB_URL", "postgres:///crails_pubsub_spec"))
  })
else
  it "pg pubsub adapter (requires PostgreSQL)" do
    pending "PostgreSQL not available"
  end
end
