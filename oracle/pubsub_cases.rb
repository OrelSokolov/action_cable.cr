# Oracle cases for cable pubsub adapters (Iteration 7).
#
# Run against the ORIGINAL gem (actioncable 8.1.4 Redis adapter, real
# redis-server):
#   TZ=UTC ruby oracle/pubsub_cases.rb > oracle/pubsub_expected.yml
#
# Each line: "<case_id>\t<value>". Mirrored 1:1 in spec/pubsub_spec.cr
# (Redis + PostgreSQL adapters run the same cases; delivery is async in
# Ruby, so cases collect into a buffer and sleep before rendering).

ENV["TZ"] = "UTC"

gem "actioncable", "= 8.1.4"
gem "redis", "= 5.4.1"

require "action_cable"
require "active_support/core_ext/hash/keys"
require "redis"

REDIS_URL = ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379/15")
Redis.new(url: REDIS_URL).flushdb

class OracleCable < ActionCable::Server::Base
  # the redis adapter calls @server.mutex (a Rails-app server detail)
  def mutex
    @mutex ||= Mutex.new
  end
end

OracleCable.config.cable = { adapter: "redis", url: REDIS_URL }
OracleCable.config.logger = Logger.new(File::NULL)

ADAPTER = ActionCable::SubscriptionAdapter::Redis.new(OracleCable.new)

def render(v)
  v.inspect
end

def case_run(id)
  puts "#{id}\t#{render(yield)}"
rescue StandardError => e
  puts "#{id}\traise:#{e.class.name}"
end

GOT = Queue.new

case_run "pubsub_roundtrip" do
  ADAPTER.subscribe("s1", ->(payload) { GOT << payload })
  sleep 0.1
  ADAPTER.broadcast("s1", "m1")
  ADAPTER.broadcast("s1", "m2")
  sleep 0.3
  collected = []
  collected << GOT.pop until GOT.empty?
  collected
end

case_run "pubsub_isolated_channels" do
  got_a = Queue.new
  got_b = Queue.new
  ADAPTER.subscribe("ch-a", ->(p) { got_a << p })
  ADAPTER.subscribe("ch-b", ->(p) { got_b << p })
  sleep 0.1
  ADAPTER.broadcast("ch-a", "for-a")
  ADAPTER.broadcast("ch-b", "for-b")
  sleep 0.3
  [got_a.pop, got_b.pop]
end

case_run "pubsub_no_delivery_without_subscription" do
  got = Queue.new
  ADAPTER.broadcast("nobody", "lost")
  sleep 0.2
  got.empty?
end

case_run "pubsub_two_subscribers_same_channel" do
  got1 = Queue.new
  got2 = Queue.new
  ADAPTER.subscribe("shared", ->(p) { got1 << p })
  ADAPTER.subscribe("shared", ->(p) { got2 << p })
  sleep 0.1
  ADAPTER.broadcast("shared", "fan")
  sleep 0.3
  [got1.pop, got2.pop]
end

case_run "pubsub_broadcast_returns" do
  ADAPTER.broadcast("ret", "x").inspect
end

ADAPTER.shutdown
