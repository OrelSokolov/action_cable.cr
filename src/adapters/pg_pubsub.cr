# ActionCable::SubscriptionAdapter::PostgreSQL — cross-process pubsub
# behind the PubsubAdapter port using PG LISTEN/NOTIFY (the monolith's
# solid_cable analog, minus its polling table: NOTIFY pushes directly).
#
# Stream names map to safe LISTEN identifiers ("ac_" + hex digest of the
# name); broadcast → pg_notify(mapped, payload). Documented limitation
# (PG itself): payload ≤ ~8000 bytes per NOTIFY. Subscriptions are
# re-registered on one dedicated listen connection whenever a new stream
# appears (messages between old/new listener are missed only in that
# window — subscribe-time, not steady-state).

require "pg"
require "digest/sha1"

module ActionCable
  module SubscriptionAdapter
    class PostgreSQL < Base
      attr_reader db : DB::Database
      attr_reader url : String

      @listen : PG::ListenConnection | Nil

      def initialize(@url : String = ENV.fetch("DATABASE_URL", "postgres:///"))
        @db = DB.open(@url)
        @subscribers = {} of String => Array(Proc(String, Nil))
        @streams_by_channel = {} of String => String
        @listen = nil
      end

      def self.connect(url : String = ENV.fetch("DATABASE_URL", "postgres:///")) : PostgreSQL
        new(url)
      end

      def broadcast(channel : String, payload : String) : Nil
        @db.exec("SELECT pg_notify($1, $2)", pg_channel(channel), payload)
      end

      def subscribe(channel : String, callback : Proc(String, Nil)) : Nil
        (@subscribers[channel] ||= [] of Proc(String, Nil)) << callback
        refresh_listener
      end

      # Same Proc-identity deviation as Test/Async: drops every callback
      # of the channel.
      def unsubscribe(channel : String, callback : Proc(String, Nil)) : Nil
        @subscribers.delete(channel)
      end

      def shutdown : Nil
        @listen.try(&.close)
        @listen = nil
        @subscribers.clear
      end

      # Deterministic stream-name → safe LISTEN identifier mapping
      # (LISTEN identifiers may not contain arbitrary bytes).
      def pg_channel(stream : String) : String
        "ac_" + Digest::SHA1.hexdigest(stream)[0, 16]
      end

      private def refresh_listener : Nil
        @listen.try(&.close)
        mapping = {} of String => String
        channels = [] of String
        @subscribers.each_key do |stream|
          ch = pg_channel(stream)
          mapping[ch] = stream
          channels << ch
        end
        @streams_by_channel = mapping
        return if channels.empty?

        @listen = PG.connect_listen(@url, channels) do |notification|
          stream = @streams_by_channel[notification.channel]?
          @subscribers[stream]?.try &.each &.call(notification.payload)
        end
      end
    end
  end
end
