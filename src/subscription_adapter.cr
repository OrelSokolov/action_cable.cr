# Subscription adapters (pubsub seam). Mirrors ActionCable::SubscriptionAdapter.
#
# Test adapter behavior per 8.1.4: broadcast records the payload for
# `broadcasts(channel)` AND delivers to subscribers (Ruby does the
# delivery asynchronously through Async; we do it synchronously —
# observable difference is only timing, values are identical).

module ActionCable
  module SubscriptionAdapter
    # Hexagonal port (Iteration 7): the narrow pubsub seam. The 1:1
    # Rails API stays on Base (broadcast/subscribe/unsubscribe/shutdown),
    # which includes this port; PG/Redis adapters implement it.
    module PubsubAdapter
    end

    class Base
      include PubsubAdapter
      def broadcast(channel : String, payload : String) : Nil
      end

      def subscribe(channel : String, callback : Proc(String, Nil)) : Nil
      end

      def unsubscribe(channel : String, callback : Proc(String, Nil)) : Nil
      end

      def shutdown : Nil
      end

      # Fast-path support (P1.5): adapters that can fan out prebuilt
      # envelopes (see Async) override this and the *_fast pair below.
      def fast_broadcasts? : Bool
        false
      end

      def subscribe_fast(channel : String, connection : Connection, identifier : String) : Nil
      end

      def unsubscribe_fast(channel : String, connection : Connection) : Nil
      end
    end

    class Test < Base
      def initialize
        @channels_data = {} of String => Array(String)
        @subscribers = {} of String => Array(Proc(String, Nil))
      end

      def broadcast(channel : String, payload : String) : Nil
        broadcasts(channel) << payload
        (@subscribers[channel]? || [] of Proc(String, Nil)).each(&.call(payload))
      end

      def broadcasts(channel : String) : Array(String)
        @channels_data[channel] ||= [] of String
      end

      def clear_messages(channel : String) : Nil
        @channels_data[channel] = [] of String
      end

      def subscribe(channel : String, callback : Proc(String, Nil)) : Nil
        (@subscribers[channel] ||= [] of Proc(String, Nil)) << callback
      end

      # Deviation from Ruby's unsubscribe(channel, callback): Proc has
      # no identity in Crystal, so this drops every subscriber of the
      # channel. Channels unsubscribe by stream name, which is the only
      # real-world caller.
      def unsubscribe(channel : String, callback : Proc(String, Nil)) : Nil
        @subscribers.delete(channel)
      end

      def shutdown : Nil
        @subscribers.clear
      end
    end
  end
end
