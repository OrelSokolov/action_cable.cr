# Async: in-process adapter with the fast-broadcast path (P1.5, see
# PLAN). Transmit-only subscriptions (stream_from without a block) are
# registered as (connection, identifier) pairs instead of Procs; a
# broadcast encodes the cable envelope once per unique identifier and
# hands the same bytes to every connection — the analog of uWS's
# TopicTree "message palette" (encode once, fan out natively).

module ActionCable
  module SubscriptionAdapter
    class Async < Base
      struct FastSub
        attr_reader connection : Connection
        attr_reader identifier : String

        def initialize(@connection, @identifier)
        end
      end

      def initialize
        @subscribers = {} of String => Array(Proc(String, Nil))
        @fast = {} of String => Array(FastSub)
      end

      def fast_broadcasts? : Bool
        true
      end

      def broadcast(channel : String, payload : String) : Nil
        if (fast = @fast[channel]?) && !fast.empty?
          groups = {} of String => Array(Connection)
          fast.each do |sub|
            (groups[sub.identifier] ||= [] of Connection) << sub.connection
          end
          groups.each do |identifier, connections|
            envelope = ActionCable.envelope_json(identifier, payload)
            connections.each &.deliver_text(envelope)
          end
        end
        (@subscribers[channel]? || [] of Proc(String, Nil)).each(&.call(payload))
      end

      def subscribe(channel : String, callback : Proc(String, Nil)) : Nil
        (@subscribers[channel] ||= [] of Proc(String, Nil)) << callback
      end

      # Same Proc-identity deviation as Test (see subscription_adapter.cr):
      # Procs have no identity in Crystal, so the whole channel is dropped.
      def unsubscribe(channel : String, callback : Proc(String, Nil)) : Nil
        @subscribers.delete(channel)
      end

      def subscribe_fast(channel : String, connection : Connection, identifier : String) : Nil
        (@fast[channel] ||= [] of FastSub) << FastSub.new(connection, identifier)
      end

      def unsubscribe_fast(channel : String, connection : Connection) : Nil
        return unless (list = @fast[channel]?)
        list.reject! { |sub| sub.connection.same?(connection) }
        @fast.delete(channel) if list.empty?
      end

      def shutdown : Nil
        @subscribers.clear
        @fast.clear
      end
    end
  end
end
