# ActionCable::SubscriptionAdapter::Redis — cross-process pubsub behind
# the PubsubAdapter port, mirroring the Rails Redis adapter:
# broadcast → PUBLISH, subscriptions → one dedicated SUBSCRIBE connection
# dispatching to stream callbacks.
#
# Documented deviations (P0): `unsubscribe` drops the channel's callbacks
# in-process but keeps the redis subscription (same Proc-identity
# limitation as Test/Async adapters); new subscriptions after the first
# are added to the same listener connection (guarded write).

require "redis"

module ActionCable
  module SubscriptionAdapter
    class Redis < Base
      @listener : ::Redis::Connection | Nil

      def initialize(url : String = ENV.fetch("REDIS_URL", "redis:///"))
        @uri = URI.parse(url)
        @client = ::Redis::Client.new(@uri)
        @subscribers = {} of String => Array(Proc(String, Nil))
        @listener = nil
        @write_lock = Mutex.new
      end

      def broadcast(channel : String, payload : String) : Nil
        @client.publish(channel, payload)
      end

      def subscribe(channel : String, callback : Proc(String, Nil)) : Nil
        first = @subscribers.empty?
        (@subscribers[channel] ||= [] of Proc(String, Nil)) << callback
        if listener = @listener
          @write_lock.synchronize { listener.subscribe(channel) }
        elsif first
          start_listener
        else
          start_listener
        end
      end

      # Same Proc-identity deviation as Test/Async: drops every callback
      # of the channel. The redis subscription itself stays (P0).
      def unsubscribe(channel : String, callback : Proc(String, Nil)) : Nil
        @subscribers.delete(channel)
      end

      def shutdown : Nil
        @listener.try(&.close)
        @listener = nil
        @subscribers.clear
      end

      private def start_listener : Nil
        conn = ::Redis::Connection.new(@uri)
        @listener = conn
        channels = @subscribers.keys
        spawn do
          begin
            conn.subscribe(channels.first) do |subscription, sub_conn|
              # add the remaining initial channels on this connection
              channels.skip(1).each { |ch| sub_conn.subscribe(ch) }
              subscription.on_message do |ch, msg|
                (@subscribers[ch]? || [] of Proc(String, Nil)).each &.call(msg)
              end
            end
          rescue ex : IO::Error | DB::PoolResourceLost
            # listener died (shutdown); nothing to do
          end
        end
      end
    end
  end
end
